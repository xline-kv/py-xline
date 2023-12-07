"""Tests for the watch client"""

import asyncio
import pytest
from client import client
from api.xline.rpc_pb2 import (
    WatchCreateRequest,
)


@pytest.mark.asyncio
async def test_watch_should_receive_consistent_events():
    """
    Watch should receive consistent events
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    kv_client = cli.kv_client
    watch_client = cli.watch_client

    watch_res, watcher_id = watch_client.watch(WatchCreateRequest(key=b"watch01"))

    async def changer():
        await kv_client.put(b"watch01", b"value01")
        await kv_client.put(b"watch01", b"value02")
        await kv_client.put(b"watch01", b"value03")

    async def watcher():
        i = 1
        async for res in watch_res:
            if res.created is True:
                watch_id = res.watch_id

            if i == 1:
                assert res.watch_id is not None
                assert res.created
            if i == 2:
                assert res.events[0].kv.key == b"watch01"
                assert res.events[0].kv.value == b"value01"
            if i == 3:
                assert res.events[0].kv.key == b"watch01"
                assert res.events[0].kv.value == b"value02"
            if i == 4:
                assert res.events[0].kv.key == b"watch01"
                assert res.events[0].kv.value == b"value03"
            if i == 5:
                assert res.watch_id is not None
                assert res.canceled

            i += 1

            watch_client.cancel(watcher_id, watch_id)

    await asyncio.gather(
        changer(),
        watcher(),
    )
