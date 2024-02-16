"""
Test for curp client
"""

from datetime import timedelta
import pytest
from src.curp.unary import UnaryBuilder, UnaryConfig
from src.curp.retry import Retry, RetryConfig
from src.rpc.type import CurpError
from curp_command_pb2 import ProposeId
from api.xline.xline_command_pb2 import Command, CommandResponse, SyncResponse
from api.xline.rpc_pb2 import RangeResponse


@pytest.mark.asyncio
async def test_unary_fetch_clusters():
    """
    Test unary fetch clusters
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    res = await unary.fetch_cluster()
    assert len(res.members) == 5
    assert res.members[0].addrs == ["A0"]
    assert res.members[1].addrs == ["A1"]
    assert res.members[2].addrs == ["A2"]
    assert res.members[3].addrs == ["A3"]
    assert res.members[4].addrs == ["A4"]


@pytest.mark.asyncio
async def test_unary_fetch_clusters_failed():
    """
    Test unary fetch clusters failed
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    try:
        await unary.fetch_cluster()
    except CurpError as e:
        assert isinstance(e, CurpError)


@pytest.mark.asyncio
async def test_fast_round_works():
    """
    Test fast round works
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    er, err = await unary.fast_round(ProposeId(seq_num=1), Command())
    assert er is not None
    assert er == CommandResponse(range_response=RangeResponse(count=1))
    assert err is None

@pytest.mark.asyncio
async def test_fast_round_return_early_err():
    """
    Test fast round return early error
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    try:
        await unary.fast_round(ProposeId(seq_num=2), Command())
    except CurpError as e:
        assert isinstance(e, CurpError)

@pytest.mark.asyncio
async def test_fast_round_less_quorum():
    """
    Test fast round less quorum
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    try:
        await unary.fast_round(ProposeId(seq_num=3), Command())
    except CurpError as e:
        assert isinstance(e, CurpError)

@pytest.mark.asyncio
async def test_fast_round_with_two_leader():
    """
    Test fast round with two leader
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    try:
        await unary.fast_round(ProposeId(seq_num=4), Command())
    except RuntimeError as e:
        assert isinstance(e, RuntimeError)

@pytest.mark.asyncio
async def test_fast_round_without_leader():
    """
    Test fast round without leader
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    try:
        await unary.fast_round(ProposeId(seq_num=5), Command())
    except RuntimeError as e:
        assert isinstance(e, RuntimeError)


@pytest.mark.asyncio
async def test_unary_slow_round_fetch_leader_first():
    """
    Test unary slow round fetch leader first
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).build()
    (er, asr), _ = await unary.slow_round(ProposeId(seq_num=6))
    assert er == CommandResponse(range_response=RangeResponse(count=1))
    assert asr == SyncResponse(revision=1)


@pytest.mark.asyncio
async def test_unary_propose_fast_path_works():
    """
    Test unary propose fast path works
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)

    unary = UnaryBuilder(all_members, config).set_leader_state(0, 1).build()
    er, asr = await unary.repeatable_propose(ProposeId(seq_num=7), Command())
    assert er == CommandResponse(range_response=RangeResponse(count=1))
    assert asr is None

@pytest.mark.asyncio
async def test_unary_propose_slow_path_works():
    """
    Test unary propose slow path works
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).set_leader_state(0, 1).build()
    er, asr = await unary.repeatable_propose(ProposeId(seq_num=7), Command(), use_fast_path=False)
    assert er == CommandResponse(range_response=RangeResponse(count=1))
    assert asr == SyncResponse(revision=1)


@pytest.mark.asyncio
async def test_unary_propose_fast_path_fallback_slow_path():
    """
    Test unary propose fast path fallback slow path
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).set_leader_state(0, 1).build()
    er, asr = await unary.repeatable_propose(ProposeId(seq_num=8), Command())
    assert er == CommandResponse(range_response=RangeResponse(count=1))
    assert asr == SyncResponse(revision=1)


@pytest.mark.asyncio
async def test_unary_propose_return_early_err():
    """
    Test unary propose return early error
    """
    all_members = {0: ["127.0.0.1:48081"]}
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).set_leader_state(0, 1).build()
    try:
        await unary.repeatable_propose(ProposeId(seq_num=9), Command())
    except CurpError as e:
        assert isinstance(e, CurpError)


@pytest.mark.asyncio
async def test_retry_propose_return_no_retry_error():
    """
    Test retry propose return no retry error
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).set_leader_state(0, 1).build()
    retry = Retry(unary, RetryConfig.new_fixed(timedelta(milliseconds=100), 3))
    try:
        await retry.propose(Command(), use_fast_path=False)
    except CurpError as e:
        assert e.inner.HasField("ShuttingDown")

@pytest.mark.asyncio
async def test_retry_propose_return_retry_error():
    """
    Test retry propose return retry error
    """
    all_members = {
        0: ["127.0.0.1:48081"],
        1: ["127.0.0.1:48082"],
        2: ["127.0.0.1:48083"],
        3: ["127.0.0.1:48084"],
        4: ["127.0.0.1:48085"],
    }
    config = UnaryConfig(1, 2)
    unary = UnaryBuilder(all_members, config).set_leader_state(0, 1).build()
    retry = Retry(unary, RetryConfig.new_fixed(timedelta(milliseconds=100), 3))
    try:
        await retry.propose(Command(), use_fast_path=False)
    except CurpError as e:
        assert e.inner.HasField("Internal")
