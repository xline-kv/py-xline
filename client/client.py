"""Xline Client"""

from __future__ import annotations
from client.protocol import ProtocolClient
from client.kv import KvClient


class Client:
    """
    Xline client

    Attributes:
        kv: Kv client
    """

    kv_client: KvClient

    def __init__(self, kv: KvClient) -> None:
        self.kv_client = kv

    @classmethod
    async def connect(cls, addrs: list[str]) -> Client:
        """
        New `Client`
        """
        protocol_client = await ProtocolClient.build_from_addrs(addrs)

        kv_client = KvClient("client", protocol_client, "")

        return cls(kv_client)
