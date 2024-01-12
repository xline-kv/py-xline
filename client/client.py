"""Xline Client"""

from __future__ import annotations
import grpc
from client.protocol import ProtocolClient
from client.kv import KvClient
from client.lease import LeaseClient, LeaseIdGenerator
from client.watch import WatchClient
from client.lock import LockClient
from client.auth import AuthClient
from client.maintenance import MaintenanceClient


class Client:
    """
    Xline client

    Attributes:
        kv_client: Kv client
        lease_client: Lease client
        watch_client: Watch client
        lock_client: Lock client
        auth_client: Auth client
        maintenance_client: Maintenance client
    """

    kv_client: KvClient
    lease_client: LeaseClient
    watch_client: WatchClient
    lock_client: LockClient
    auth_client: AuthClient
    maintenance_client: MaintenanceClient

    def __init__(
        self,
        kv: KvClient,
        lease: LeaseClient,
        watch: WatchClient,
        lock: LockClient,
        auth: AuthClient,
        maintenance: MaintenanceClient,
    ) -> None:
        self.kv_client = kv
        self.lease_client = lease
        self.watch_client = watch
        self.lock_client = lock
        self.auth_client = auth
        self.maintenance_client = maintenance

    @classmethod
    async def connect(cls, addrs: list[str]) -> Client:
        """
        New `Client`
        """
        protocol_client = await ProtocolClient.build_from_addrs(addrs)
        # TODO: Load balancing
        channel = grpc.aio.insecure_channel(addrs[0])
        # TODO: Acquire the auth token
        id_gen = LeaseIdGenerator()

        kv_client = KvClient(protocol_client, "")
        lease_client = LeaseClient(protocol_client, channel, "", id_gen)
        watch_client = WatchClient(channel)
        lock_client = LockClient(protocol_client, channel, "", id_gen)
        auth_client = AuthClient(protocol_client, channel, "")
        maintenance_client = MaintenanceClient(channel)

        return cls(kv_client, lease_client, watch_client, lock_client, auth_client, maintenance_client)
