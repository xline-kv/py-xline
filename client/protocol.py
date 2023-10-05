"""Protocol client"""

from __future__ import annotations
import asyncio
import logging
from typing import Union

import grpc
from google.protobuf.internal.containers import RepeatedCompositeFieldContainer

from api.curp import curp_command_pb2, curp_error_pb2, message_pb2, message_pb2_grpc
from api.xline import xline_command_pb2, xline_error_pb2

ProposeError = Union[
    curp_error_pb2.ProposeError,
    xline_error_pb2.ExecuteError,
    curp_error_pb2.CommandSyncError,
    curp_error_pb2.WaitSyncError,
    str,
]


class ProtocolClient:
    """
    Protocol client

    Attributes:
        local_server_id: local server id. Only use in an inner client.
        state: state of a client
        inner: inner protocol clients
        connects: all servers's `Connect`
    """

    local_server_id: int
    inner: list[message_pb2_grpc.ProtocolStub]
    connects: RepeatedCompositeFieldContainer[message_pb2.Member]

    def __init__(
        self,
        leader_id: int,
        stubs: list[message_pb2_grpc.ProtocolStub],
        connects: RepeatedCompositeFieldContainer[message_pb2.Member],
    ) -> None:
        self.local_server_id = leader_id
        self.inner = stubs
        self.connects = connects

    @classmethod
    def build_from_addrs(cls, addrs: list[str]) -> ProtocolClient:
        """Build client from addresses, this method will fetch all members from servers"""

        stubs: list[message_pb2_grpc.ProtocolStub] = []

        for addr in addrs:
            channel = grpc.insecure_channel(addr)
            stub = message_pb2_grpc.ProtocolStub(channel)
            stubs.append(stub)

        cluster = fetch_cluster(stubs)

        return cls(
            cluster.leader_id,
            stubs,
            cluster.members,
        )

    def propose(
        self, cmd: xline_command_pb2.Command, use_fast_path: bool = False
    ) -> tuple[
        tuple[xline_command_pb2.CommandResponse, xline_command_pb2.SyncResponse | None] | None,
        ProposeError | None,
    ]:
        """Propose the request to servers, if use_fast_path is false, it will wait for the synced index"""

        if use_fast_path:
            fast_round_res, _ = asyncio.run(self.fast_round(cmd))
            slow_round_res, slow_round_err = asyncio.run(self.slow_round(cmd))
            if fast_round_res is not None:
                return ((fast_round_res, None), None)
            elif slow_round_res is not None:
                sync_res, cmd_res = slow_round_res
                return ((cmd_res, sync_res), None)
            else:
                return (None, slow_round_err)
        else:
            asyncio.run(self.fast_round(cmd))
            slow_round_res, slow_round_err = asyncio.run(self.slow_round(cmd))
            if slow_round_res is not None:
                sync_res, cmd_res = slow_round_res
                return ((cmd_res, sync_res), None)
            else:
                return (None, slow_round_err)

    async def fast_round(
        self, cmd: xline_command_pb2.Command
    ) -> tuple[
        xline_command_pb2.CommandResponse | None,
        xline_error_pb2.ExecuteError | curp_error_pb2.ProposeError | None,
    ]:
        """
        The fast round of Curp protocol
        It broadcast the requests to all the curp servers.
        """

        logging.info("fast round start. propose id: %s", cmd.propose_id)

        ok_cnt = 0
        is_received_leader_res = False
        is_received_success_res = None
        cmd_res = xline_command_pb2.CommandResponse()
        exe_err = xline_error_pb2.ExecuteError()
        propose_err = curp_error_pb2.ProposeError()

        for stub in self.inner:
            res = await propose_wrapper(stub, cmd)

            if res.result is not None:
                cmd_result = res.result
                ok_cnt += 1
                is_received_leader_res = True
                if len(cmd_result.er) != 0:
                    is_received_success_res = True
                    cmd_res.ParseFromString(cmd_result.er)
                if len(cmd_result.error) != 0:
                    is_received_success_res = False
                    exe_err.ParseFromString(cmd_result.error)
            if res.error is not None:
                is_received_leader_res = True
                propose_err.CopyFrom(res.error)
            else:
                logging.warning("propose response prase fail")

            if is_received_leader_res and ok_cnt >= super_quorum(len(self.connects)):
                logging.info("fast round success")
                if is_received_success_res:
                    return (cmd_res, None)
                if not is_received_success_res:
                    return (None, exe_err)

        return (None, propose_err)

    async def slow_round(
        self, cmd: xline_command_pb2.Command
    ) -> tuple[
        tuple[xline_command_pb2.SyncResponse, xline_command_pb2.CommandResponse] | None,
        curp_error_pb2.CommandSyncError | curp_error_pb2.WaitSyncError | None,
    ]:
        """The slow round of Curp protocol"""

        logging.info("slow round start. propose id: %s", cmd.propose_id)

        addr = ""
        sync_res = xline_command_pb2.SyncResponse()
        cmd_res = xline_command_pb2.CommandResponse()
        exe_err = curp_error_pb2.CommandSyncError()
        after_sync_err = curp_error_pb2.WaitSyncError()

        for member in self.connects:
            if member.id == self.local_server_id:
                addr = member.name
                break

        channel = grpc.insecure_channel(addr)
        stub = message_pb2_grpc.ProtocolStub(channel)
        res = await wait_synced_wrapper(stub, cmd)

        if len(res.success.after_sync_result) != 0:
            success = res.success
            sync_res.ParseFromString(success.after_sync_result)
            cmd_res.ParseFromString(success.exe_result)
            return ((sync_res, cmd_res), None)
        if len(res.error.execute) != 0 or len(res.error.after_sync) != 0:
            cmd_sync_err = res.error
            if len(cmd_sync_err.execute) != 0:
                exe_err.ParseFromString(cmd_sync_err.execute)
                return (None, exe_err)
            if len(cmd_sync_err.after_sync) != 0:
                after_sync_err.ParseFromString(cmd_sync_err.after_sync)
                return (None, after_sync_err)

        return (None, None)


def fetch_cluster(stubs: list[message_pb2_grpc.ProtocolStub]) -> message_pb2.FetchClusterResponse:
    """Fetch cluster from server"""
    for stub in stubs:
        res: message_pb2.FetchClusterResponse = stub.FetchCluster(message_pb2.FetchClusterRequest())
    return res


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


async def propose_wrapper(
    stub: message_pb2_grpc.ProtocolStub, req: xline_command_pb2.Command
) -> curp_command_pb2.ProposeResponse:
    """Wrapper of propose"""
    res: curp_command_pb2.ProposeResponse = stub.Propose(
        curp_command_pb2.ProposeRequest(command=req.SerializeToString())
    )
    return res


async def wait_synced_wrapper(
    stub: message_pb2_grpc.ProtocolStub, req: xline_command_pb2.Command
) -> curp_command_pb2.WaitSyncedResponse:
    """Wrapper of wait sync"""
    res: curp_command_pb2.WaitSyncedResponse = stub.WaitSynced(
        curp_command_pb2.WaitSyncedRequest(propose_id=req.propose_id)
    )
    return res
