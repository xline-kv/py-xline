"""Cluster Client"""

from grpc import Channel
from api.xline.rpc_pb2_grpc import ClusterStub
from api.xline.rpc_pb2 import (
    MemberAddRequest,
    MemberAddResponse,
    MemberRemoveRequest,
    MemberRemoveResponse,
    MemberPromoteRequest,
    MemberPromoteResponse,
    MemberUpdateRequest,
    MemberUpdateResponse,
    MemberListRequest,
    MemberListResponse,
)


class ClusterClient:
    """
    Client for cluster client.

    Attributes:
        inner: The cluster RPC client, only communicate with one server at a time
    """

    inner: ClusterStub

    def __init__(self, channel: Channel) -> None:
        self.inner = ClusterStub(channel=channel)

    async def member_add(self, peer_urls: list[str], is_learner: bool = False) -> MemberAddResponse:
        """Add a new member to the cluster."""
        request = MemberAddRequest(peerURLs=peer_urls, isLearner=is_learner)
        response = await self.inner.MemberAdd(request)
        return response

    async def member_remove(self, member_id: int) -> MemberRemoveResponse:
        """Remove an existing member from the cluster."""
        request = MemberRemoveRequest(ID=member_id)
        response = await self.inner.MemberRemove(request)
        return response

    async def member_promote(self, member_id: int) -> MemberPromoteResponse:
        """Promote an existing member to be the leader of the cluster."""
        request = MemberPromoteRequest(ID=member_id)
        response = await self.inner.MemberPromote(request)
        return response

    async def member_update(self, member_id: int, peer_urls: list[str]) -> MemberUpdateResponse:
        """Update an existing member in the cluster."""
        request = MemberUpdateRequest(ID=member_id, peerURLs=peer_urls)
        response = await self.inner.MemberUpdate(request)
        return response

    async def member_list(self, linearizable: bool = False) -> MemberListResponse:
        """List all members in the cluster."""
        request = MemberListRequest(linearizable=linearizable)
        response = await self.inner.MemberList(request)
        return response
