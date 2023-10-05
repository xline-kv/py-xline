"""Xline Client"""

from typing import List
from client.protocol import ProtocolClient
from client.kv import KvClient


class Client:
    """
    Xline client

    Attributes:
        kv: Kv client
    """

    kv_client: KvClient

    def __init__(self, addrs: List[str]) -> None:
        protocol_client = ProtocolClient.build_from_addrs(addrs)

        kv_client = KvClient("client", protocol_client, "")

        self.kv_client = kv_client
