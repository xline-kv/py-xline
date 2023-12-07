"""Tests for the auth client"""

import pytest
from client import client
from client.auth import PERM_READ, PERM_WRITE, PERM_READWRITE
from api.xline.auth_pb2 import Permission


@pytest.mark.asyncio
async def test_user_operations_should_success_in_normal_path():
    """
    user operations should success in normal path.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    auth_client = cli.auth_client

    name1 = "usr1"
    password1 = "pwd1"
    password2 = "pwd2"

    await auth_client.user_add(name1, password1)
    await auth_client.user_get(name1)

    res = await auth_client.user_list()
    assert res.users[0] == "usr1"

    await auth_client.user_change_password(name1, password2)

    await auth_client.user_delete(name1)


@pytest.mark.asyncio
async def test_role_operations_should_success_in_normal_path():
    """
    test_role_operations_should_success_in_normal_path
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    auth_client = cli.auth_client

    role1 = "role1"
    role2 = "role2"

    await auth_client.role_add(role1)
    await auth_client.role_add(role2)

    await auth_client.role_get(role1)
    await auth_client.role_get(role2)

    role_list_res = await auth_client.role_list()
    assert role_list_res.roles[0] == role1
    assert role_list_res.roles[1] == role2

    await auth_client.role_delete(role1)
    await auth_client.role_delete(role2)

    try:
        await auth_client.role_get(role1)
    except BaseException as e:
        assert e is not None
    try:
        await auth_client.role_get(role2)
    except BaseException as e:
        assert e is not None


@pytest.mark.asyncio
async def test_user_role_operations_should_success_in_normal_path():
    """
    User role operations should success in normal path.
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    auth_client = cli.auth_client

    name1 = "usr1"
    role1 = "role1"
    role2 = "role2"

    await auth_client.user_add(name1)
    await auth_client.role_add(role1)
    await auth_client.role_add(role2)

    await auth_client.user_grant_role(name1, role1)
    await auth_client.user_grant_role(name1, role2)

    res = await auth_client.user_get(name1)
    assert any(role in (role1, role2) for role in res.roles)

    await auth_client.user_revoke_role(name1, role1)
    await auth_client.user_revoke_role(name1, role2)
    await auth_client.role_delete(role1)
    await auth_client.role_delete(role2)
    await auth_client.user_delete(name1)


@pytest.mark.asyncio
async def test_permission_operations_should_success_in_normal_path():
    """
    Permission operations should success in normal path
    """
    curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
    cli = await client.Client.connect(curp_members)
    auth_client = cli.auth_client

    role1 = "role1"
    perm1 = Permission(permType=Permission.READ, key=b"123")
    perm2 = Permission(permType=Permission.WRITE, key=b"abc", range_end=b"\x00")
    perm3 = Permission(permType=Permission.READWRITE, key=b"hi", range_end=b"hjj")
    perm4 = Permission(permType=Permission.WRITE, key=b"pp", range_end=b"pq")
    perm5 = Permission(permType=Permission.WRITE, key=b"\x00", range_end=b"\x00")

    await auth_client.role_add(role1)
    await auth_client.role_grant_permission(role1, PERM_READ, b"123")
    await auth_client.role_grant_permission(role1, PERM_WRITE, b"abc", b"\x00")
    await auth_client.role_grant_permission(role1, PERM_READWRITE, b"hi", b"hjj")
    await auth_client.role_grant_permission(role1, PERM_WRITE, b"pp", b"pq")
    await auth_client.role_grant_permission(role1, PERM_WRITE, b"\x00", b"\x00")

    res = await auth_client.role_get(role1)
    assert all(perm in res.perm for perm in (perm1, perm2, perm3, perm4, perm5))

    # revoke all permission
    await auth_client.role_revoke_permission(role1, b"123")
    await auth_client.role_revoke_permission(role1, b"abc", b"\x00")
    await auth_client.role_revoke_permission(role1, b"hi", b"hjj")
    await auth_client.role_revoke_permission(role1, b"pp", b"pq")
    await auth_client.role_revoke_permission(role1, b"\x00", b"\x00")

    res = await auth_client.role_get(role1)
    assert len(res.perm) == 0

    await auth_client.role_delete(role1)
