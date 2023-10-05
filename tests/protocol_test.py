"""Tests for the protocol client."""

import unittest
import uuid

from api.xline.xline_command_pb2 import Command, RequestWithToken
from api.xline.rpc_pb2 import PutRequest
from client.protocol import ProtocolClient


class TestProtocolClient(unittest.TestCase):
    """test protocol client"""

    def setUp(self) -> None:
        curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]

        cmd = Command(
            request=RequestWithToken(
                put_request=PutRequest(
                    key=bytes("hello", encoding="utf8"),
                    value=bytes("xline", encoding="utf8"),
                )
            ),
            propose_id=f"client-{uuid.uuid4()}",
        )

        self.cmd = cmd
        self.client = ProtocolClient.build_from_addrs(curp_members)

    def test_fast_path(self):
        """test fast path"""
        res = self.client.propose(self.cmd, True)
        self.assertIsNotNone(res)

    def test_slow_path(self):
        """test slow path"""
        res = self.client.propose(self.cmd, False)
        self.assertIsNotNone(res)
