"""Tests for the lease client"""

import pytest
from client import client
from api.xline.rpc_pb2 import (
    LeaseGrantRequest,
    LeaseRevokeRequest,
)


@pytest.mark.asyncio
async def test_grant_revoke_should_success_in_normal_path():
    """
    Grant revoke should success in normal path.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    lease_client = cli.lease_client

    res = await lease_client.grant(LeaseGrantRequest(TTL=123))
    assert res.TTL == 123

    lease_id = res.ID
    await lease_client.revoke(LeaseRevokeRequest(ID=lease_id))


@pytest.mark.asyncio
async def test_keep_alive_should_success_in_normal_path():
    """
    Keep alive should success in normal path.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    lease_client = cli.lease_client

    grant_res = await lease_client.grant(LeaseGrantRequest(TTL=60))
    lease_id = grant_res.ID

    responses, keep_id = await lease_client.keep_alive(lease_id)

    async for res in responses:
        assert res.ID == lease_id
        assert res.TTL == 60

        lease_client.cancel_keep_alive(keep_id)

    await lease_client.revoke(LeaseRevokeRequest(ID=lease_id))


@pytest.mark.asyncio
async def test_leases_should_include_granted_in_normal_path():
    """
    Leases should include granted in normal path.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    lease_client = cli.lease_client

    lease1 = 100
    lease2 = 101
    lease3 = 102

    await lease_client.grant(LeaseGrantRequest(TTL=60, ID=lease1))
    await lease_client.grant(LeaseGrantRequest(TTL=60, ID=lease2))
    await lease_client.grant(LeaseGrantRequest(TTL=60, ID=lease3))

    res = await lease_client.leases()
    assert len(res.leases) == 3

    await lease_client.revoke(LeaseRevokeRequest(ID=lease1))
    await lease_client.revoke(LeaseRevokeRequest(ID=lease2))
    await lease_client.revoke(LeaseRevokeRequest(ID=lease3))
