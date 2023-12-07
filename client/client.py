"""Xline Client"""

from __future__ import annotations
import grpc
from client.protocol import ProtocolClient
from client.kv import KvClient
from client.auth import AuthClient


class Client:
    """
    Xline client

    Attributes:
        kv_client: Kv client
        auth_client: Auth client
    """

    kv_client: KvClient
    auth_client: AuthClient

    def __init__(self, kv: KvClient, auth: AuthClient) -> None:
        self.kv_client = kv
        self.auth_client = auth

    @classmethod
    async def connect(cls, addrs: list[str]) -> Client:
        """
        New `Client`
        """
        protocol_client = await ProtocolClient.build_from_addrs(addrs)
        # TODO: Load balancing
        channel = grpc.aio.insecure_channel(addrs[0])
        # TODO: Acquire the auth token

        kv_client = KvClient("client", protocol_client, "")
        auth_client = AuthClient("client", protocol_client, channel, "")

        return cls(kv_client, auth_client)
