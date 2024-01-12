"""
Protocol Client
"""

from __future__ import annotations
import asyncio
import logging
import grpc
import random

from api.curp.curp_command_pb2 import (
    ProposeRequest,
    ProposeResponse,
    WaitSyncedRequest,
    WaitSyncedResponse,
    ProposeId,
    FetchClusterResponse,
    FetchClusterRequest,
)
from api.curp.curp_command_pb2_grpc import ProtocolStub
from client.error import ExecuteError
from api.xline.xline_command_pb2 import Command, CommandResponse, SyncResponse
from api.xline.xline_error_pb2 import ExecuteError as _ExecuteError


class ProtocolClient:
    """
    Protocol client

    Attributes:
        state: Current leader and term
        connects: `all servers's `Connect`
        cluster_version: Cluster version
    """

    state: State
    connects: dict[int, grpc.Channel]
    cluster_version: int
    # TODO config

    def __init__(
        self,
        state: State,
        connects: dict[int, grpc.Channel],
        cluster_version: int,
    ) -> None:
        self.state = state
        self.connects = connects
        self.cluster_version = cluster_version

    @classmethod
    async def build_from_addrs(cls, addrs: list[str]) -> ProtocolClient:
        """
        Build client from addresses, this method will fetch all members from servers
        """
        cluster = await cls.fast_fetch_cluster(addrs)

        connects = {}
        for member in cluster.members:
            channel = grpc.aio.insecure_channel(member.addrs[0])
            connects[member.id] = channel

        return cls(
            State(cluster.leader_id, cluster.term),
            connects,
            cluster.cluster_version,
        )

    @staticmethod
    async def fast_fetch_cluster(addrs: list[str]) -> FetchClusterResponse:
        """
        Fetch cluster from server, return the first `FetchClusterResponse
        """
        futures = []
        for addr in addrs:
            channel = grpc.aio.insecure_channel(addr)
            stub = ProtocolStub(channel)
            futures.append(stub.FetchCluster(FetchClusterRequest()))
        for t in asyncio.as_completed(futures):
            return await t

        msg = "fetch cluster error"
        raise Exception(msg)

    async def propose(self, cmd: Command, use_fast_path: bool = False) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Propose the request to servers, if use_fast_path is false, it will wait for the synced index
        """
        propose_id = self.gen_propose_id()

        # TODO: retry
        if use_fast_path:
            return await self.fast_path(propose_id, cmd)
        else:
            return await self.slow_path(propose_id, cmd)
        # TODO: error handling

    async def fast_path(self, propose_id: ProposeId, cmd: Command) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Fast path of propose
        """
        fast_round = self.fast_round(propose_id, cmd)
        slow_round = self.slow_round(propose_id)

        # Wait for the fast and slow round at the same time
        for futures in asyncio.as_completed([fast_round, slow_round]):
            try:
                first, second = await futures
            except Exception as e:
                logging.warning(e)
                continue

            if isinstance(first, CommandResponse) and second:
                # TODO: error handling
                return (first, None)
            if isinstance(second, CommandResponse) and isinstance(first, SyncResponse):
                # TODO: error handling
                return (second, first)

        msg = "fast path error"
        raise Exception(msg)

    async def slow_path(self, propose_id: ProposeId, cmd: Command) -> tuple[CommandResponse, SyncResponse]:
        """
        Slow path of propose
        """
        results = await asyncio.gather(self.fast_round(propose_id, cmd), self.slow_round(propose_id))
        for result in results:
            if isinstance(result[0], SyncResponse) and isinstance(result[1], CommandResponse):
                return (result[1], result[0])

        msg = "slow path error"
        raise Exception(msg)

    async def fast_round(self, propose_id: ProposeId, cmd: Command) -> tuple[CommandResponse | None, bool]:
        """
        The fast round of Curp protocol
        It broadcast the requests to all the curp servers.
        """
        logging.info("fast round start. propose id: %s", propose_id)

        ok_cnt = 0
        is_received_leader_res = False
        cmd_res = CommandResponse()
        exe_err = ExecuteError(_ExecuteError())

        futures = []
        for server_id in self.connects:
            stub = ProtocolStub(self.connects[server_id])
            futures.append(stub.Propose(ProposeRequest(propose_id=propose_id, command=cmd.SerializeToString())))

        for future in asyncio.as_completed(futures):
            res: ProposeResponse = ProposeResponse()
            try:
                res = await future
            except Exception as e:
                logging.warning(e)
                continue

            ok_cnt += 1
            if not res.HasField("result"):
                continue
            cmd_result = res.result
            if cmd_result.HasField("ok"):
                if is_received_leader_res:
                    msg = "should not set exe result twice"
                    raise Exception(msg)
                cmd_res.ParseFromString(cmd_result.ok)
                is_received_leader_res = True
            elif cmd_result.HasField("error"):
                exe_err.inner.ParseFromString(cmd_result.error)
                raise exe_err

            if is_received_leader_res and ok_cnt >= self.super_quorum(len(self.connects)):
                logging.info("fast round succeed. propose id: %s", propose_id)
                return (cmd_res, True)

        logging.info("fast round failed. propose id: %s", propose_id)
        return (cmd_res, False)

    async def slow_round(self, propose_id: ProposeId) -> tuple[SyncResponse, CommandResponse]:
        """
        The slow round of Curp protocol
        """
        logging.info("slow round start. propose id: %s", propose_id)

        asr = SyncResponse()
        er = CommandResponse()
        exe_err = ExecuteError(_ExecuteError())

        channel = self.connects[self.state.leader]
        stub = ProtocolStub(channel)
        res: WaitSyncedResponse = await stub.WaitSynced(
            WaitSyncedRequest(propose_id=propose_id, cluster_version=self.cluster_version)
        )

        if res.after_sync_result.ok:
            asr.ParseFromString(res.after_sync_result.ok)
        elif res.after_sync_result.error:
            exe_err.inner.ParseFromString(res.after_sync_result.error)
            raise exe_err

        if res.exe_result.ok:
            er.ParseFromString(res.exe_result.ok)
        elif res.exe_result.error:
            exe_err.inner.ParseFromString(res.exe_result.error)
            raise exe_err

        return (asr, er)

    def gen_propose_id(self) -> ProposeId:
        """
        Generate a propose id
        """
        client_id = self.get_client_id()
        seq_sum = self.new_seq_num()
        return ProposeId(client_id=client_id, seq_num=seq_sum)

    def new_seq_num(self) -> int:
        """
        New a seq num and record it

        TODO: implement request tracker
        """
        return 0

    def get_client_id(self) -> int:
        """
        Get the client id

        TODO: grant a client id from server
        """
        return random.randint(0, 2**64 - 1)

    @staticmethod
    def super_quorum(nodes: int) -> int:
        """
        Get the superquorum for curp protocol
        Although curp can proceed with f + 1 available replicas, it needs f + 1 + (f + 1)/2 replicas
        (for superquorum of witnesses) to use 1 RTT operations. With less than superquorum replicas,
        clients must ask masters to commit operations in f + 1 replicas before returning result.(2 RTTs).
        """
        fault_tolerance = nodes // 2
        quorum = fault_tolerance + 1
        superquorum = fault_tolerance + (quorum // 2) + 1
        return superquorum


ServerId = int


class State:
    """
    Protocol client state

    Attributes:
        leader: Current leader
        term: Current term
    """

    leader: int
    term: int

    def __init__(self, leader: int, term: int) -> None:
        self.leader = leader
        self.term = term
