"""
Protocol Client
"""

from __future__ import annotations
import asyncio
import logging
import grpc

from api.curp.message_pb2_grpc import ProtocolStub
from api.curp.message_pb2 import FetchClusterRequest, FetchClusterResponse
from api.curp.curp_command_pb2 import ProposeRequest, WaitSyncedRequest
from api.xline.xline_command_pb2 import Command, CommandResponse, SyncResponse
from client.error import ResDecodeError, CommandSyncError, WaitSyncError, ExecuteError
from api.curp.curp_error_pb2 import (
    CommandSyncError as _CommandSyncError,
    WaitSyncError as _WaitSyncError,
)
from api.xline.xline_error_pb2 import ExecuteError as _ExecuteError


class ProtocolClient:
    """
    Protocol client

    Attributes:
        leader_id: cluster `leader id`
        connects: `all servers's `Connect`
    """

    leader_id: int
    connects: dict[int, grpc.Channel]

    def __init__(
        self,
        leader_id: int,
        connects: dict[int, grpc.Channel],
    ) -> None:
        self.leader_id = leader_id
        self.connects = connects

    @classmethod
    async def build_from_addrs(cls, addrs: list[str]) -> ProtocolClient:
        """
        Build client from addresses, this method will fetch all members from servers
        """
        cluster = await cls.fast_fetch_cluster(addrs)

        connects = {}
        for member in cluster.members:
            channel = grpc.aio.insecure_channel(member.name)
            connects[member.id] = channel

        return cls(
            cluster.leader_id,
            connects,
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
        if use_fast_path:
            return await self.fast_path(cmd)
        else:
            return await self.slow_path(cmd)

    async def fast_path(self, cmd: Command) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Fast path of propose
        """
        for futures in asyncio.as_completed([self.fast_round(cmd), self.slow_round(cmd)]):
            first, second = await futures
            if isinstance(first, CommandResponse) and second:
                return (first, None)
            if isinstance(second, CommandResponse) and isinstance(first, SyncResponse):
                return (second, first)

        msg = "fast path error"
        raise Exception(msg)

    async def slow_path(self, cmd: Command) -> tuple[CommandResponse, SyncResponse]:
        """
        Slow path of propose
        """
        results = await asyncio.gather(self.fast_round(cmd), self.slow_round(cmd))
        for result in results:
            if isinstance(result[0], SyncResponse) and isinstance(result[1], CommandResponse):
                return (result[1], result[0])

        msg = "slow path error"
        raise Exception(msg)

    async def fast_round(self, cmd: Command) -> tuple[CommandResponse | None, bool]:
        """
        The fast round of Curp protocol
        It broadcast the requests to all the curp servers.
        """
        logging.info("fast round start. propose id: %s", cmd.propose_id)

        ok_cnt = 0
        is_received_leader_res = False
        cmd_res = CommandResponse()
        exe_err = ExecuteError(_ExecuteError())

        futures = []
        for server_id in self.connects:
            stub = ProtocolStub(self.connects[server_id])
            futures.append(stub.Propose(ProposeRequest(command=cmd.SerializeToString())))

        for future in asyncio.as_completed(futures):
            res = await future

            if res.HasField("result"):
                cmd_result = res.result
                ok_cnt += 1
                is_received_leader_res = True
                if cmd_result.HasField("er"):
                    cmd_res.ParseFromString(cmd_result.er)
                if cmd_result.HasField("error"):
                    exe_err.inner.ParseFromString(cmd_result.error)
                    raise exe_err
            elif res.HasField("error"):
                logging.info(res.error)
            else:
                ok_cnt += 1

            if is_received_leader_res and ok_cnt >= self.super_quorum(len(self.connects)):
                logging.info("fast round succeed. propose id: %s", cmd.propose_id)
                return (cmd_res, True)

        logging.info("fast round failed. propose id: %s", cmd.propose_id)
        return (cmd_res, False)

    async def slow_round(self, cmd: Command) -> tuple[SyncResponse, CommandResponse]:
        """
        The slow round of Curp protocol
        """
        logging.info("slow round start. propose id: %s", cmd.propose_id)

        sync_res = SyncResponse()
        cmd_res = CommandResponse()
        exe_err = CommandSyncError(_CommandSyncError())
        after_sync_err = WaitSyncError(_WaitSyncError())

        channel = self.connects[self.leader_id]
        stub = ProtocolStub(channel)
        res = await stub.WaitSynced(WaitSyncedRequest(propose_id=cmd.propose_id))

        if res.HasField("success"):
            success = res.success
            sync_res.ParseFromString(success.after_sync_result)
            cmd_res.ParseFromString(success.exe_result)
            logging.info("slow round succeed. propose id: %s", cmd.propose_id)
            return (sync_res, cmd_res)
        if res.HasField("error"):
            cmd_sync_err = res.error
            if cmd_sync_err.HasField("execute"):
                exe_err.inner.ParseFromString(cmd_sync_err.execute)
                raise exe_err
            if cmd_sync_err.HasField("after_sync"):
                after_sync_err.inner.ParseFromString(cmd_sync_err.after_sync)
                raise after_sync_err

        err_msg = "Response decode error"
        raise ResDecodeError(err_msg)

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
