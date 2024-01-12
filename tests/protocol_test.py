"""Tests for the protocol client."""

import pytest

from api.xline.xline_command_pb2 import Command, RequestWithToken
from api.xline.rpc_pb2 import PutRequest
from client.protocol import ProtocolClient


@pytest.mark.asyncio
async def test_propose_fast_path():
    """
    test propose fast path
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    client = await ProtocolClient.build_from_addrs(curp_members)
    cmd = Command(
        request=RequestWithToken(
            put_request=PutRequest(
                key=b"hello",
                value=b"py-xline",
            )
        ),
    )

    er, _ = await client.propose(cmd, True)
    assert er.put_response is not None


@pytest.mark.asyncio
async def test_propose_slow_path():
    """
    test propose slow path
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    client = await ProtocolClient.build_from_addrs(curp_members)
    cmd = Command(
        request=RequestWithToken(
            put_request=PutRequest(
                key=b"hello1",
                value=b"py-xline1",
            )
        ),
    )

    er, asr = await client.propose(cmd, False)
    assert asr is not None
    assert er.put_response is not None
