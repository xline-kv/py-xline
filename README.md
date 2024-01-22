# py-xline

[![Apache 2.0 licensed][apache-badge]][apache-url]
[![codecov][cov-badge]][cov-url]

[apache-badge]: https://img.shields.io/badge/license-Apache--2.0-brightgreen
[apache-url]: https://github.com/datenlord/Xline/blob/master/LICENSE
[cov-badge]: https://codecov.io/gh/xline-kv/xline/branch/master/graph/badge.svg
[cov-url]: https://codecov.io/gh/xline-kv/go-xline

py-xline is an official xline client sdk, written in Python.

## Features

`py-xline` runs the CURP protocol on the client side for maximal performance.

## Supported APIs

- KV
  - [x] Put
  - [x] Range
  - [x] Delete
  - [x] Txn
  - [x] Compact
- Auth
  - [x] AuthEnable
  - [x] AuthDisable
  - [x] AuthStatus
  - [x] Authenticate
  - [x] UserAdd
  - [x] UserAddWithOptions
  - [x] UserGet
  - [x] UserList
  - [x] UserDelete
  - [x] UserChangePassword
  - [x] UserGrantRole
  - [x] UserRevokeRole
  - [x] RoleAdd
  - [x] RoleGet
  - [x] RoleList
  - [x] RoleGrantPermission
  - [x] RoleRevokePermission
- Lease
  - [x] Grant
  - [x] Revoke
  - [x] KeepAlive
  - [x] KeepAliveOnce
  - [x] TimeToLive
  - [x] Leases
- Watch
  - [x] Watch
- Lock
  - [x] Lock
  - [x] Unlock
- Cluster
  - [ ] MemberAdd
  - [ ] MemberAddAsLearner
  - [ ] MemberRemove
  - [ ] MemberUpdate
  - [ ] MemberList
  - [ ] MemberPromote
- Maintenance
  - [ ] Alarm
  - [ ] Status
  - [ ] Defragment
  - [ ] Hash
  - [x] Snapshot
  - [ ] MoveLeader

## Installation

``` bash
pip install py-xline
```

## Quickstart

``` python
from py_xline import client

async def main():
  curp_members = ["172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"]
  cli = await client.Client.connect(curp_members)
  kv_client = cli.kv_client

  await kv_client.put(b"key", b"value")

  res = await kv_client.range(b"key")

  kv = res.kvs[0]
  print(kv.key.decode(), kv.value.decode())
```

## Compatibility

We aim to maintain compatibility with each corresponding Xline version, and update this library with each new Xline release.

| py-xline | Xline |
| --- | --- |
| v0.1.0 | v0.6.1 |
