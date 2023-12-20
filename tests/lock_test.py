"""Tests for the lock client"""

import pytest
import asyncio
from client import client


@pytest.mark.asyncio
async def test_lock_unlock_should_success_in_normal_path():
    """
    Test lock unlock should success in normal path
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    lock_client = cli.lock_client

    res = await lock_client.lock(b"lock-test")
    assert res.key.decode().startswith("lock-test/")

    await lock_client.unlock(b"lock-test")


@pytest.mark.asyncio
async def test_lock_contention_should_occur_when_acquire_by_two():
    """
    Test lock contention should occur when acquire by two
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    lock_client = cli.lock_client

    await lock_client.lock(b"lock-test")

    try:
        await asyncio.wait_for(lock_client.lock(b"lock-test"), timeout=1)
        msg = "Lock contention should occur when acquire by two"
        raise AssertionError(msg)
    except asyncio.TimeoutError:
        pass

    await lock_client.unlock(b"lock-test")


@pytest.mark.asyncio
async def test_lock_should_timeout_when_ttl_is_set():
    """
    Test lock should timeout when ttl is set
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    lock_client = cli.lock_client

    await lock_client.lock(b"lock-test", ttl=2)

    try:
        await asyncio.wait_for(lock_client.lock(b"lock-test"), timeout=2)
        msg = "Lock should timeout when ttl is set"
        raise AssertionError(msg)
    except asyncio.TimeoutError:
        pass

    try:
        await asyncio.wait_for(lock_client.lock(b"lock-test"), timeout=0.5)
    except asyncio.TimeoutError as e:
        raise e

    await lock_client.unlock(b"lock-test")


@pytest.mark.asyncio
async def test_lock_should_unlock_successfully():
    """
    Test lock should unlock successfully
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    lock_client = cli.lock_client

    await lock_client.lock(b"lock-test")

    await lock_client.unlock(b"lock-test")

    try:
        await asyncio.wait_for(lock_client.lock(b"lock-test"), timeout=0.2)
    except asyncio.TimeoutError as e:
        raise e
