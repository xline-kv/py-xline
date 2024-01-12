"""Tests for the cluster client"""

import pytest
from client import client


@pytest.mark.asyncio
async def test_cluster_add_members():
    """
    Test cluster add members.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    cluster_client = cli.cluster_client

    res = await cluster_client.member_add(["172.20.0.6:2379"])
    id1 = res.member.ID
    assert res.member.peerURLs[0] == "172.20.0.6:2379"
    assert len(res.members) == 4

    res = await cluster_client.member_add(["172.20.0.7:2379"], is_learner=True)
    id2 = res.member.ID
    assert res.member.peerURLs[0] == "172.20.0.7:2379"
    assert res.member.isLearner
    assert len(res.members) == 5

    await cluster_client.member_remove(id1)
    await cluster_client.member_remove(id2)


@pytest.mark.asyncio
async def test_cluster_remove_members():
    """
    Test cluster remove members.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    cluster_client = cli.cluster_client

    res = await cluster_client.member_add(["172.20.0.6:2379"])
    member_id = res.member.ID

    res = await cluster_client.member_remove(member_id)
    assert len(res.members) == 3


@pytest.mark.asyncio
async def test_cluster_promote_member():
    """
    Test cluster promote member.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    cluster_client = cli.cluster_client

    res = await cluster_client.member_add(["172.20.0.6:2379"], is_learner=True)
    member_id = res.member.ID

    res = await cluster_client.member_promote(member_id)
    for member in res.members:
        if member.ID == member_id:
            assert not member.isLearner

    await cluster_client.member_remove(member_id)


@pytest.mark.asyncio
async def test_cluster_update_member():
    """
    Test_cluster_update_member.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    cluster_client = cli.cluster_client

    res = await cluster_client.member_add(["172.20.0.6:2379"])
    member_id = res.member.ID

    res = await cluster_client.member_update(member_id, ["172.20.0.7:2379"])
    for member in res.members:
        if member.ID == member_id:
            assert member.peerURLs[0] == "172.20.0.7:2379"

    await cluster_client.member_remove(member_id)


@pytest.mark.asyncio
async def test_cluster_member_list():
    """
    Test cluster member list.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    cluster_client = cli.cluster_client

    res = await cluster_client.member_add(["172.20.0.6:2379"])
    await cluster_client.member_add(["172.20.0.7:2379"])

    res = await cluster_client.member_list()
    assert len(res.members) == 5
