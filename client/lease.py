"""Lease Client"""

import asyncio
import random
import uuid
from typing import Optional
from grpc import Channel
from grpc.aio import StreamStreamCall
from client.protocol import ProtocolClient as CurpClient
from api.xline.xline_command_pb2 import Command, RequestWithToken
from api.xline.rpc_pb2_grpc import LeaseStub
from api.xline.rpc_pb2 import (
    LeaseGrantRequest,
    LeaseGrantResponse,
    LeaseRevokeRequest,
    LeaseRevokeResponse,
    LeaseKeepAliveRequest,
    LeaseTimeToLiveRequest,
    LeaseTimeToLiveResponse,
    LeaseLeasesRequest,
    LeaseLeasesResponse,
)


class LeaseIdGenerator:
    """
    Generator of unique lease id
    Note that this Lease Id generation method may cause collisions,
    the client should retry after informed by the server.

    Attributes:
        id: The current lease id.
    """

    lease_id: int

    def __init__(self) -> None:
        self.lease_id = int.from_bytes(random.randbytes(8), "big")

    def next(self) -> int:
        """Generate a new `leaseId`."""
        lease_id = self.lease_id
        self.lease_id += 1
        if lease_id == 0:
            return self.next()
        return lease_id & 0x7FFF_FFFF_FFFF_FFFF


class LeaseKeeper:
    """
    Keeper of lease

    Attributes:
        id: The current lease id.
        remote: The lease RPC client.
        is_cancel: Whether the keeper is canceled.
    """

    id: str
    remote: LeaseStub
    is_cancel: bool

    def __init__(self, remote: LeaseStub) -> None:
        self.id = ""
        self.remote = remote
        self.is_cancel = False

    def keep(self, req: LeaseKeepAliveRequest) -> StreamStreamCall:
        """Keep alive"""

        async def keep():
            while not self.is_cancel:
                yield req
                await asyncio.sleep(1)

        res = self.remote.LeaseKeepAlive(keep())
        return res

    def cancel(self) -> None:
        """Cancel the keeper"""
        self.is_cancel = True


class LeaseClient:
    """
    Client for Lease operations.

    Attributes:
        curp_client: The client running the CURP protocol, communicate with all servers.
        lease_client: The lease RPC client, only communicate with one server at a time.
        token: The auth token.
        id_gen: Lease Id generator.
        keepers: Keep alive keepers.
    """

    curp_client: CurpClient
    lease_client: LeaseStub
    token: Optional[str]
    id_gen: LeaseIdGenerator
    keepers: dict[str, LeaseKeeper]

    def __init__(
        self, curp_client: CurpClient, channel: Channel, token: Optional[str], id_gen: LeaseIdGenerator
    ) -> None:
        self.curp_client = curp_client
        self.lease_client = LeaseStub(channel=channel)
        self.token = token
        self.id_gen = id_gen
        self.keepers = {}

    async def grant(self, req: LeaseGrantRequest) -> LeaseGrantResponse:
        """
        Creates a lease which expires if the server does not receive a keepAlive
        within a given time to live period. All keys attached to the lease will be expired and
        deleted if the lease expires. Each expired key generates a delete event in the event history.
        """
        if req.ID == 0:
            req.ID = self.id_gen.next()
        request_with_token = RequestWithToken(token=self.token, lease_grant_request=req)
        cmd = Command(
            request=request_with_token,
        )
        er, _ = await self.curp_client.propose(cmd, True)
        return er.lease_grant_response

    async def revoke(self, req: LeaseRevokeRequest) -> LeaseRevokeResponse:
        """
        Revokes a lease. All keys attached to the lease will expire and be deleted.
        """
        res: LeaseRevokeResponse = await self.lease_client.LeaseRevoke(req)
        return res

    async def keep_alive(self, lease_id: int):
        """
        Keeps the lease alive by streaming keep alive requests from the client
        to the server and streaming keep alive responses from the server to the client.
        """
        keeper = LeaseKeeper(self.lease_client)

        keep_id = str(uuid.uuid4())
        self.keepers[keep_id] = keeper

        res = keeper.keep(LeaseKeepAliveRequest(ID=lease_id))
        return res, keep_id

    def cancel_keep_alive(self, keep_id: str):
        """Cancel keep alive"""
        if keep_id in self.keepers:
            self.keepers[keep_id].cancel()
            del self.keepers[keep_id]

    def time_to_live(self, req: LeaseTimeToLiveRequest) -> LeaseTimeToLiveResponse:
        """
        Retrieves lease information.
        """
        res: LeaseTimeToLiveResponse = self.lease_client.LeaseTimeToLive(req)
        return res

    async def leases(self) -> LeaseLeasesResponse:
        """
        Lists all existing leases.
        """
        request_with_token = RequestWithToken(token=self.token, lease_leases_request=LeaseLeasesRequest())
        cmd = Command(
            request=request_with_token,
        )
        er, _ = await self.curp_client.propose(cmd, True)
        return er.lease_leases_response
