"""Tests for the kv client"""

import pytest
from client import client, kv, txn


@pytest.mark.asyncio
async def test_put_should_success_in_normal_path():
    """
    test put should success in normal path
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    kv_client = cli.kv_client

    await kv_client.put(b"put", b"123")

    # overwrite with prev key
    res = await kv_client.put(b"put", b"456", prev_kv=True)
    prev_kv = res.prev_kv
    assert prev_kv is not None
    assert prev_kv.key == b"put"
    assert prev_kv.value == b"123"

    # overwrite again with prev key
    res = await kv_client.put(b"put", b"456", prev_kv=True)
    prev_kv = res.prev_kv
    assert prev_kv is not None
    assert prev_kv.key == b"put"
    assert prev_kv.value == b"456"


@pytest.mark.asyncio
async def test_range_should_fetches_previously_put_keys():
    """
    test_range_should_fetches_previously_put_keys
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    kv_client = cli.kv_client

    await kv_client.put(b"get10", b"10")
    await kv_client.put(b"get11", b"11")
    await kv_client.put(b"get20", b"20")
    await kv_client.put(b"get21", b"21")

    # get key
    res = await kv_client.range(b"get11")
    assert res.count == 1
    assert not res.more
    assert len(res.kvs) == 1
    assert res.kvs[0].key == b"get11"
    assert res.kvs[0].value == b"11"

    # get from key
    res = await kv_client.range(b"get11", range_end=b"\0", limit=2)
    assert res.more
    assert len(res.kvs) == 2
    assert res.kvs[0].key == b"get11"
    assert res.kvs[0].value == b"11"
    assert res.kvs[1].key == b"get20"
    assert res.kvs[1].value == b"20"


@pytest.mark.asyncio
async def test_delete_should_remove_previously_put_kvs():
    """
    test_delete_should_remove_previously_put_kvs
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    kv_client = cli.kv_client

    await kv_client.put(b"del10", b"10")
    await kv_client.put(b"del11", b"11")
    await kv_client.put(b"del20", b"20")
    await kv_client.put(b"del21", b"21")
    await kv_client.put(b"del31", b"31")
    await kv_client.put(b"del32", b"32")

    # delete key
    del_res = await kv_client.delete(b"del11", prev_kv=True)
    assert del_res.deleted == 1
    assert del_res.prev_kvs[0].key == b"del11"
    assert del_res.prev_kvs[0].value == b"11"

    range_res = await kv_client.range(b"del11", count_only=True)
    assert range_res.count == 0

    # delete a range of keys
    del_res = await kv_client.delete(b"del11", b"del22", prev_kv=True)
    assert del_res.deleted == 2
    assert del_res.prev_kvs[0].key == b"del20"
    assert del_res.prev_kvs[0].value == b"20"
    assert del_res.prev_kvs[1].key == b"del21"
    assert del_res.prev_kvs[1].value == b"21"

    range_res = await kv_client.range(b"del11", range_end=b"del22", count_only=True)
    assert range_res.count == 0


@pytest.mark.asyncio
async def test_compact_should_remove_previous_revision():
    """
    test_compact_should_remove_previous_revision
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    kv_client = cli.kv_client

    await kv_client.put(b"compact", b"0")
    put_res = await kv_client.put(b"compact", b"1")
    rev = put_res.header.revision

    # before compacting
    rev0_res = await kv_client.range(b"compact", revision=rev - 1)
    assert rev0_res.kvs[0].value == b"0"

    rev1_res = await kv_client.range(b"compact", revision=rev)
    assert rev1_res.kvs[0].value == b"1"

    await kv_client.compact(rev)

    # after compacting
    try:
        await kv_client.range(b"compact", revision=rev - 1)
    except BaseException as e:
        assert e is not None

    range_res = await kv_client.range(b"compact", revision=rev)
    assert range_res.kvs[0].value == b"1"


@pytest.mark.asyncio
async def test_txn_should_execute_as_expected():
    """
    test_txn_should_execute_as_expected
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    kv_client = cli.kv_client

    await kv_client.put(b"txn01", b"01")

    # transaction 1
    txn_res = await (
        kv_client.txn()
        .when(
            [
                txn.Compare(
                    key=b"txn01",
                    value=b"01",
                    target=txn.Compare.CompareTarget.VALUE,
                    result=txn.Compare.CompareResult.EQUAL,
                )
            ]
        )
        .and_then([txn.RequestOp(request_put=txn.PutRequest(key=b"txn01", value=b"02", prev_kv=True))])
        .or_else([txn.RequestOp(request_range=txn.RangeRequest(key=b"txn01"))])
        .commit()
    )

    assert txn_res.succeeded
    op_res = txn_res.responses
    assert len(op_res) == 1
    assert isinstance(op_res[0].response_put, kv.PutResponse)
    put_res = op_res[0].response_put
    assert put_res.prev_kv.value == b"01"

    # transaction 2
    txn_res = await (
        kv_client.txn()
        .when(
            [
                txn.Compare(
                    key=b"txn01",
                    value=b"01",
                    target=txn.Compare.CompareTarget.VALUE,
                    result=txn.Compare.CompareResult.EQUAL,
                )
            ]
        )
        .and_then([txn.RequestOp(request_put=txn.PutRequest(key=b"txn01", value=b"02"))])
        .or_else([txn.RequestOp(request_range=txn.RangeRequest(key=b"txn01"))])
        .commit()
    )

    assert not txn_res.succeeded
    op_res = txn_res.responses
    assert len(op_res) == 1
    assert isinstance(op_res[0].response_range, kv.RangeResponse)
    range_res = op_res[0].response_range
    assert range_res.kvs[0].value == b"02"
