"""Tests for the kv client"""

import unittest
from client import client
from api.xline.rpc_pb2 import (
    PutRequest,
    PutResponse,
    RangeRequest,
    RangeResponse,
    DeleteRangeRequest,
    TxnRequest,
    Compare,
    RequestOp,
    CompactionRequest,
)


class TestKvClient(unittest.TestCase):
    """test kv client"""

    def setUp(self) -> None:
        curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
        kv_client = client.Client(curp_members).kv_client
        self.kv_client = kv_client

    def test_put_should_success_in_normal_path(self):
        """
        test put should success in normal path
        """
        self.kv_client.put(PutRequest(key=b"put", value=b"123"))

        # overwrite with prev key
        res, _ = self.kv_client.put(PutRequest(key=b"put", value=b"456", prev_kv=True))
        prev_kv = res.prev_kv
        self.assertIsNotNone(prev_kv)
        self.assertEqual(prev_kv.key, b"put")
        self.assertEqual(prev_kv.value, b"123")

        # overwrite again with prev key
        res, _ = self.kv_client.put(PutRequest(key=b"put", value=b"456", prev_kv=True))
        prev_kv = res.prev_kv
        self.assertIsNotNone(prev_kv)
        self.assertEqual(prev_kv.key, b"put")
        self.assertEqual(prev_kv.value, b"456")

    def test_range_should_fetches_previously_put_keys(self):
        """
        test range should fetches previously put keys
        """
        self.kv_client.put(PutRequest(key=b"get10", value=b"10"))
        self.kv_client.put(PutRequest(key=b"get11", value=b"11"))
        self.kv_client.put(PutRequest(key=b"get20", value=b"20"))
        self.kv_client.put(PutRequest(key=b"get21", value=b"21"))

        # get key
        res, _ = self.kv_client.range(RangeRequest(key=b"get11"))
        self.assertEqual(res.count, 1)
        self.assertFalse(res.more)
        self.assertEqual(len(res.kvs), 1)
        self.assertEqual(res.kvs[0].key, b"get11")
        self.assertEqual(res.kvs[0].value, b"11")

        # get from key
        res, _ = self.kv_client.range(RangeRequest(key=b"get11", range_end=b"\0", limit=2))
        self.assertTrue(res.more)
        self.assertEqual(len(res.kvs), 2)
        self.assertEqual(res.kvs[0].key, b"get11")
        self.assertEqual(res.kvs[0].value, b"11")
        self.assertEqual(res.kvs[1].key, b"get20")
        self.assertEqual(res.kvs[1].value, b"20")

    def test_delete_should_remove_previously_put_kvs(self):
        """
        test_delete_should_remove_previously_put_kvs
        """
        self.kv_client.put(PutRequest(key=b"del10", value=b"10"))
        self.kv_client.put(PutRequest(key=b"del11", value=b"11"))
        self.kv_client.put(PutRequest(key=b"del20", value=b"20"))
        self.kv_client.put(PutRequest(key=b"del21", value=b"21"))
        self.kv_client.put(PutRequest(key=b"del31", value=b"31"))
        self.kv_client.put(PutRequest(key=b"del32", value=b"32"))

        # delete key
        del_res, _ = self.kv_client.delete(DeleteRangeRequest(key=b"del11", prev_kv=True))
        self.assertEqual(del_res.deleted, 1)
        self.assertEqual(del_res.prev_kvs[0].key, b"del11")
        self.assertEqual(del_res.prev_kvs[0].value, b"11")

        range_res, _ = self.kv_client.range(RangeRequest(key=b"del11", count_only=True))
        self.assertEqual(range_res.count, 0)

        # delete a range of keys
        del_res, _ = self.kv_client.delete(DeleteRangeRequest(key=b"del11", range_end=b"del22", prev_kv=True))
        self.assertEqual(del_res.deleted, 2)
        self.assertEqual(del_res.prev_kvs[0].key, b"del20")
        self.assertEqual(del_res.prev_kvs[0].value, b"20")
        self.assertEqual(del_res.prev_kvs[1].key, b"del21")
        self.assertEqual(del_res.prev_kvs[1].value, b"21")

        range_res, _ = self.kv_client.range(RangeRequest(key=b"del11", range_end=b"del22", count_only=True))
        self.assertEqual(range_res.count, 0)

    def test_txn_should_execute_as_expected(self):
        """
        txn_should_execute_as_expected
        """
        self.kv_client.put(PutRequest(key=b"txn01", value=b"01"))

        # transaction 1
        txn_res, _ = self.kv_client.txn(
            TxnRequest(
                compare=[
                    Compare(
                        key=b"txn01",
                        value=b"01",
                        target=Compare.CompareTarget.VALUE,
                        result=Compare.CompareResult.EQUAL,
                    )
                ],
                success=[RequestOp(request_put=PutRequest(key=b"txn01", value=b"02", prev_kv=True))],
                failure=[RequestOp(request_range=RangeRequest(key=b"txn01"))],
            )
        )
        self.assertTrue(txn_res.succeeded)
        op_res = txn_res.responses
        self.assertEqual(len(op_res), 1)
        self.assertEqual(type(op_res[0].response_put), PutResponse)
        put_res = op_res[0].response_put
        self.assertEqual(put_res.prev_kv.value, b"01")

        range_res, _ = self.kv_client.range(RangeRequest(key=b"txn01"))
        self.assertEqual(range_res.kvs[0].key, b"txn01")
        self.assertEqual(range_res.kvs[0].value, b"02")

        # transaction 2
        txn_res, _ = self.kv_client.txn(
            TxnRequest(
                compare=[
                    Compare(
                        key=b"txn01",
                        value=b"01",
                        target=Compare.CompareTarget.VALUE,
                        result=Compare.CompareResult.EQUAL,
                    )
                ],
                success=[
                    RequestOp(
                        request_put=PutRequest(
                            key=b"txn01",
                            value=b"02",
                        )
                    )
                ],
                failure=[RequestOp(request_range=RangeRequest(key=b"txn01"))],
            )
        )
        self.assertFalse(txn_res.succeeded)
        op_res = txn_res.responses
        self.assertEqual(len(op_res), 1)
        self.assertEqual(type(op_res[0].response_range), RangeResponse)
        range_res = op_res[0].response_range
        self.assertEqual(range_res.kvs[0].value, b"02")

    def test_compact_should_remove_previous_revision(self):
        """
        compact_should_remove_previous_revision
        """
        self.kv_client.put(PutRequest(key=b"compact", value=b"0"))
        put_res, _ = self.kv_client.put(PutRequest(key=b"compact", value=b"1"))
        rev = put_res.header.revision

        # before compacting
        rev0_res, _ = self.kv_client.range(RangeRequest(key=b"compact", revision=rev - 1))
        self.assertEqual(rev0_res.kvs[0].value, b"0")

        rev1_res, _ = self.kv_client.range(RangeRequest(key=b"compact", revision=rev))
        self.assertEqual(rev1_res.kvs[0].value, b"1")

        self.kv_client.compact(CompactionRequest(revision=rev))

        # after compacting
        _, err = self.kv_client.range(RangeRequest(key=b"compact", revision=rev - 1))
        self.assertIsNotNone(err)

        range_res, _ = self.kv_client.range(RangeRequest(key=b"compact", revision=rev))
        self.assertEqual(range_res.kvs[0].value, b"1")
