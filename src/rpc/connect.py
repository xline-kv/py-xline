"""
Rpc connect
"""

from __future__ import annotations
import threading
import asyncio
import grpc
import curp_command_pb2
import curp_command_pb2_grpc


class Connect:
    """
    The connection struct to hold the real rpc connections,
    it may failed to Connect, but it also retries the next time
    """

    # Server serverId
    __server_id: int
    # The rpc connection
    __rpc_connect: list[grpc.Channel]
    # The current rpc connection address, when the address is updated,
    # `addrs` will be used to remove previous connection
    __addrs: list[str]
    # The lock
    __lock: threading.Lock

    def __init__(self, server_id: int, rpc_connect: list[grpc.Channel], addrs: list[str]) -> None:
        self.__server_id = server_id
        self.__rpc_connect = rpc_connect
        self.__addrs = addrs
        self.__lock = threading.Lock()

    @classmethod
    def connect(cls, server_id: int, addrs: list[str]) -> Connect:
        """
        A wrapper of `connect_to`
        """
        return cls.__connect_to(server_id, addrs)

    @classmethod
    def connects(cls, member: dict[int, list[str]]) -> dict[int, list[grpc.Channel]]:
        """
        Wrapper of `connect_all`
        """
        return cls.__connect_all(member)

    @classmethod
    def __connect_to(cls, server_id: int, addrs: list[str]) -> Connect:
        """
        Connect to a server
        """
        # TODO: load balancing
        # TODO: support TLS
        channels: list[grpc.Channel] = []
        for addr in addrs:
            channel = grpc.aio.insecure_channel(addr)
            channels.append(channel)
        return cls(server_id, channels, addrs)

    @classmethod
    def __connect_all(cls, member: dict[int, list[str]]) -> dict[int, list[grpc.Channel]]:
        """
        Connect to a map of members
        """
        channels: dict[int, list[grpc.Channel]] = {}
        for server_id, addrs in member.items():
            conns = cls.__connect_to(server_id, addrs)
            channels[server_id] = conns
        return channels

    def update_addrs(self, addrs: list[str]):
        """
        Update server addresses, the new addresses will override the old ones
        """
        self.__inner_update_addrs(addrs)

    def __inner_update_addrs(self, addrs: list[str]):
        """
        Update addresses
        """
        with self.__lock:
            old_addrs = set(self.__addrs)
            new_addrs = set(addrs)

            diffs = old_addrs.symmetric_difference(new_addrs)

            for diff in diffs:
                if diff in new_addrs:
                    self.__addrs.append(diff)
                else:
                    self.__addrs.remove(diff)

    async def propose(
        self,
        req: curp_command_pb2.ProposeRequest,
        timeout: float,
    ) -> curp_command_pb2.ProposeResponse:
        """
        Send `ProposeRequest`
        """
        futures: list[asyncio.Future[curp_command_pb2.ProposeResponse]] = []
        for conn in self.__rpc_connect:
            stub = curp_command_pb2_grpc.ProtocolStub(conn)
            future: asyncio.Future[curp_command_pb2.ProposeResponse] = stub.Propose(req, timeout=timeout)
            futures.append(future)

        for future in asyncio.as_completed(futures):
            res: curp_command_pb2.ProposeResponse = await future
            return res

    async def wait_synced(
        self,
        req: curp_command_pb2.WaitSyncedRequest,
        timeout: float,
    ) -> curp_command_pb2.WaitSyncedResponse:
        """
        Send `WaitSyncedRequest`
        """
        futures: list[asyncio.Future[curp_command_pb2.WaitSyncedResponse]] = []
        for conn in self.__rpc_connect:
            stub = curp_command_pb2_grpc.ProtocolStub(conn)
            future: asyncio.Future[curp_command_pb2.WaitSyncedResponse] = stub.WaitSynced(req, timeout=timeout)
            futures.append(future)

        for future in asyncio.as_completed(futures):
            res: curp_command_pb2.WaitSyncedResponse = await future
            return res

    async def fetch_cluster(
        self,
        req: curp_command_pb2.FetchClusterRequest,
        timeout: float,
    ) -> curp_command_pb2.FetchClusterResponse:
        """
        Send `FetchClusterRequest`
        """
        futures: list[asyncio.Future[curp_command_pb2.FetchClusterResponse]] = []
        for conn in self.__rpc_connect:
            stub = curp_command_pb2_grpc.ProtocolStub(conn)
            future: asyncio.Future[curp_command_pb2.FetchClusterResponse] = stub.FetchCluster(req, timeout=timeout)
            futures.append(future)

        for future in asyncio.as_completed(futures):
            res: curp_command_pb2.WaitSyncedResponse = await future
            return res
