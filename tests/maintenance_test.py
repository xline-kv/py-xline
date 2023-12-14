"""Test for the maintenance client"""

import pytest
from client import client


@pytest.mark.asyncio
async def test_snapshot_should_get_valid_data():
    """
    Snapshot should get valid data
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    maintenance_client = cli.maintenance_client

    res = await maintenance_client.snapshot()
    async for snapshot in res:
        assert snapshot.blob != b""
