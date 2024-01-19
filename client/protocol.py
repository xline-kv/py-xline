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
    CurpError,
)
from api.curp.curp_command_pb2_grpc import ProtocolStub
from client.error import (
    ExecuteError,
    WrongClusterVersionError,
    ShuttingDownError,
    InternalError,
)
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
            if len(member.addrs) == 0:
                msg = "a member must have at least one addr"
                raise Exception(msg)
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

    async def fetch_cluster(self, linearizable: bool) -> FetchClusterResponse:
        """
        Send fetch cluster requests to all servers
        Note: The fetched cluster may still be outdated if `linearizable` is false
        """
        connects = self.all_connects()
        rpcs: list[grpc.Future[FetchClusterResponse]] = []

        for channel in connects:
            stub = ProtocolStub(channel)
            rpcs.append(stub.FetchCluster(FetchClusterRequest(linearizable=linearizable)))

        max_term = 0
        resp: FetchClusterResponse | None = None
        ok_cnt = 0
        majority_cnt = len(connects) // 2 + 1

        for rpc in asyncio.as_completed(rpcs):
            try:
                res: FetchClusterResponse = await rpc
            except grpc.RpcError as e:
                logging.warning(e)
                continue

            if max_term < res.term:
                max_term = res.term
                if len(res.members) == 0:
                    resp = res
                ok_cnt = 1
            elif max_term == res.term:
                if len(res.members) == 0:
                    resp = res
                ok_cnt += 1
            else:
                pass

            if ok_cnt >= majority_cnt:
                break

        if resp is not None:
            logging.debug("Fetch cluster succeeded, result: %s", res)
            self.state.check_and_update(res.leader_id, res.term)
            return resp

    async def fetch_leader(self) -> ServerId:
        """
        Send fetch leader requests to all servers until there is a leader
        Note: The fetched leader may still be outdated
        """
        res = await self.fetch_cluster(False)
        return res.leader_id

    async def propose(self, cmd: Command, use_fast_path: bool = False) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Propose the request to servers, if use_fast_path is false, it will wait for the synced index
        """
        propose_id = self.gen_propose_id()

        if use_fast_path:
            return await self.fast_path(propose_id, cmd)
        else:
            return await self.slow_path(propose_id, cmd)

    async def fast_path(self, propose_id: ProposeId, cmd: Command) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Fast path of propose
        """
        fast_round = self.fast_round(propose_id, cmd)
        slow_round = self.slow_round(propose_id, cmd)

        # Wait for the fast and slow round at the same time
        for futures in asyncio.as_completed([fast_round, slow_round]):
            try:
                first, second = await futures
            except Exception as e:
                logging.warning(e)
                continue

            if isinstance(first, CommandResponse) and second:
                return (first, None)
            if isinstance(second, CommandResponse) and isinstance(first, SyncResponse):
                return (second, first)

        msg = "fast path error"
        raise Exception(msg)

    async def slow_path(self, propose_id: ProposeId, cmd: Command) -> tuple[CommandResponse, SyncResponse]:
        """
        Slow path of propose
        """
        results = await asyncio.gather(self.fast_round(propose_id, cmd), self.slow_round(propose_id, cmd))
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
            except grpc.RpcError as e:
                logging.warning(e)
                curp_err = CurpError()
                dtl: str = e.details()
                try:
                    curp_err.ParseFromString(dtl.encode())
                except Exception as e:
                    logging.warning(e)
                    continue
                if curp_err.HasField("ShuttingDown"):
                    raise ShuttingDownError from e
                elif curp_err.HasField("WrongClusterVersion"):
                    raise WrongClusterVersionError from e
                else:
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

    async def slow_round(self, propose_id: ProposeId, cmd: Command) -> tuple[SyncResponse, CommandResponse]:
        """
        The slow round of Curp protocol
        """
        logging.info("slow round start. propose id: %s", propose_id)

        asr = SyncResponse()
        er = CommandResponse()
        exe_err = ExecuteError(_ExecuteError())

        channel = self.connects[self.state.leader]
        stub = ProtocolStub(channel)

        res = WaitSyncedResponse()
        try:
            res: WaitSyncedResponse = await stub.WaitSynced(
                WaitSyncedRequest(propose_id=propose_id, cluster_version=self.cluster_version)
            )
        except grpc.RpcError as e:
            logging.warning("wait synced rpc error: %s", e)
            curp_err = CurpError()
            dtl: str = e.details()
            curp_err.ParseFromString(dtl.encode())
            if curp_err.HasField("ShuttingDown"):
                raise ShuttingDownError from e
            elif curp_err.HasField("WrongClusterVersion"):
                raise WrongClusterVersionError from e
            elif curp_err.HasField("RpcTransport"):
                # it's quite likely that the leader has crashed,
                # then we should wait for some time and fetch the leader again
                self.resend_propose(propose_id, cmd, None)
            elif curp_err.HasField("redirect"):
                new_leader = curp_err.redirect.leader_id
                term = curp_err.redirect.term
                self.state.check_and_update(new_leader, term)
                # resend the propose to the new leader
                self.resend_propose(propose_id, cmd, None)
            else:
                raise InternalError from e

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

    async def resend_propose(self, propose_id: ProposeId, cmd: Command, new_leader: ServerId | None) -> True | None:
        """
        Resend the propose only to the leader.
        This is used when leader changes and we need to ensure that the propose is received by the new leader.
        """
        leader_id: int | None = None
        if new_leader is not None:
            leader_id = new_leader
            try:
                res = await self.fetch_leader()
                leader_id = res
            except Exception as e:
                logging.warning("failed to fetch leader, %s", e)
        logging.debug("resend propose to %s", leader_id)

        stub = ProtocolStub(self.get_connect(leader_id))

        try:
            stub.Propose(
                ProposeRequest(
                    propose_id=propose_id, command=cmd.SerializeToString(), cluster_version=self.cluster_version
                )
            )
        except grpc.RpcError as e:
            # if the propose fails again, need to fetch the leader and try again
            logging.warning("failed to resend propose, %s", e)
            curp_err = CurpError()
            dtl: str = e.details()
            curp_err.ParseFromString(dtl.encode())
            if curp_err.HasField("ShuttingDown"):
                raise ShuttingDownError from e
            elif curp_err.HasField("WrongClusterVersion"):
                raise WrongClusterVersionError from e
            elif curp_err.HasField("Duplicated"):
                return True
            else:
                return None

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

    def all_connects(self) -> list[grpc.Channel]:
        """
        Get all connects
        """
        return list(self.connects.values())

    def get_connect(self, _id: ServerId) -> grpc.Channel:
        """
        Get all connects
        """
        return self.connects[_id]

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

    leader: int | None
    term: int

    def __init__(self, leader: int | None, term: int) -> None:
        self.leader = leader
        self.term = term

    def check_and_update(self, leader_id: int | None, term: int):
        """
        Check the term and leader id, update the state if needed
        """
        if self.term < term:
            # reset term only when the resp has leader id to prevent:
            # If a server loses contact with its leader, it will update its term for election.
            # Since other servers are all right, the election will not succeed.
            # But if the client learns about the new term and updates its term to it, it will never get the true leader.
            if leader_id is not None:
                new_leader_id = leader_id
                self.update_to_term(term)
                self.set_leader(new_leader_id)
        elif self.term == term:
            if leader_id is not None:
                new_leader_id = leader_id
                if self.leader is None:
                    self.set_leader(new_leader_id)
        else:
            pass

    def update_to_term(self, term: int) -> None:
        """
        Update to the newest term and reset local cache
        """
        self.term = term
        self.leader = None

    def set_leader(self, _id: ServerId) -> None:
        """
        Set the leader and notify all the waiters
        """
        logging.debug("client update its leader to %s", _id)
        self.leader = _id
