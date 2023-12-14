"""Maintenance Client"""

from typing import AsyncIterable
from grpc import Channel
from api.xline.rpc_pb2_grpc import MaintenanceStub
from api.xline.rpc_pb2 import SnapshotRequest, SnapshotResponse


class MaintenanceClient:
    """
    Client for Maintenance operations.

    Attributes:
        maintenance_client: The client running the Maintenance protocol, communicate with all servers.
    """

    maintenance_client: MaintenanceStub

    def __init__(self, channel: Channel) -> None:
        self.maintenance_client = MaintenanceStub(channel)

    async def snapshot(self) -> AsyncIterable[SnapshotResponse]:
        """Gets a snapshot over a stream"""
        res = self.maintenance_client.Snapshot(SnapshotRequest())
        return res
