"""Lock Client"""

from urllib import parse
from typing import Optional
from grpc import Channel
from client.protocol import ProtocolClient as CurpClient
from client.lease import LeaseClient, LeaseIdGenerator
from client.watch import WatchClient
from api.xline.xline_command_pb2 import Command, RequestWithToken, CommandResponse, SyncResponse
from api.xline.v3lock_pb2 import LockRequest as _LockRequest, LockResponse, UnlockResponse
from api.xline.kv_pb2 import Event
from api.xline.rpc_pb2 import (
    PutRequest,
    RangeRequest,
    LeaseGrantRequest,
    TxnRequest,
    Compare,
    RequestOp,
    ResponseHeader,
    WatchCreateRequest,
    DeleteRangeRequest,
)


class LockRequest:
    """
    Request for `Lock`

    Attributes:
        inner: The inner request.
        ttl: The ttl of the lease that attached to the lock.
    """

    inner: _LockRequest
    ttl: Optional[int]

    def __init__(self, req: _LockRequest, ttl: int = 60) -> None:
        self.inner = req
        self.ttl = ttl


class LockClient:
    """
    Client for Lock operations.

    Attributes:
        curp_client: The client running the CURP protocol, communicate with all servers.
        lease_client: The lease client.
        watch_client: The watch client.
        token: Auth token
    """

    curp_client: CurpClient
    lease_client: LeaseClient
    watch_client: WatchClient
    token: Optional[str]

    def __init__(self, curp_client: CurpClient, channel: Channel, token: str, id_gen: LeaseIdGenerator) -> None:
        self.curp_client = curp_client
        self.lease_client = LeaseClient(curp_client=curp_client, channel=channel, token=token, id_gen=id_gen)
        self.watch_client = WatchClient(channel=channel)
        self.token = token

    async def lock(self, name: bytes, lease_id: int = 0, ttl: Optional[int] = None) -> LockResponse:
        """
        Acquires a distributed shared lock on a given named lock.
        On success, it will return a unique key that exists so long as the
        lock is held by the caller. This key can be used in conjunction with
        transactions to safely ensure updates to Xline only occur while holding
        lock ownership. The lock is held until Unlock is called on the key or the
        lease associate with the owner expires.
        """
        if lease_id == 0:
            lease_res = await self.lease_client.grant(LeaseGrantRequest(TTL=ttl))
            lease_id = lease_res.ID

        prefix = f"{parse.quote(name)}/"
        key = f"{prefix}{lease_id:x}"
        res = await self.lock_inner(prefix, key, lease_id)

        return res

    async def unlock(self, key: bytes) -> UnlockResponse:
        """
        Takes a key returned by Lock and releases the hold on lock. The
        next Lock caller waiting for the lock will then be woken up and given
        ownership of the lock.
        """
        header = await self.delete_key(key)
        return UnlockResponse(header=header)

    async def lock_inner(self, prefix: str, key: str, lease_id: int) -> LockResponse:
        """
        The inner lock logic
        """
        txn = self.create_acquire_txn(prefix, lease_id)
        req = RequestWithToken(token=self.token, txn_request=txn)
        er, asr = await self.propose(req, False)

        txn_res = er.txn_response
        if asr is None:
            msg = "sync_res always has value when use slow path"
            raise Exception(msg)
        my_rev = asr.revision
        owner_res = txn_res.responses[1].response_range
        owner_key = owner_res.kvs

        header = ResponseHeader()
        if len(owner_key) > 0 and owner_key[0].create_revision == my_rev:
            header = owner_res.header
        else:
            await self.wait_delete(prefix, my_rev)
            range_req = RangeRequest(key=key.encode())
            req = RequestWithToken(token=self.token, range_request=range_req)
            try:
                er, _ = await self.propose(req, True)
                range_res = er.range_response
                if len(range_res.kvs) == 0:
                    msg = "rpc error session expired"
                    raise Exception(msg)
                header = range_res.header
            except Exception:
                await self.delete_key(key.encode())

        return LockResponse(header=header, key=key.encode())

    def create_acquire_txn(self, prefix: str, lease_id: int) -> TxnRequest:
        """
        Create txn for try acquire lock
        """
        key = f"{prefix}{lease_id:x}"
        cmp = Compare(
            result=Compare.CompareResult.EQUAL, target=Compare.CompareTarget.CREATE, key=key.encode(), range_end=b""
        )
        put = RequestOp(request_put=PutRequest(key=key.encode(), value=b"", lease=lease_id))
        get = RequestOp(request_range=RangeRequest(key=key.encode()))
        range_end = self.get_prefix(key.encode())
        get_owner = RequestOp(
            request_range=RangeRequest(
                key=prefix.encode(),
                range_end=range_end,
                sort_order=RangeRequest.SortOrder.ASCEND,
                sort_target=RangeRequest.SortTarget.CREATE,
                limit=1,
            )
        )
        return TxnRequest(compare=[cmp], success=[put, get_owner], failure=[get, get_owner])

    def get_prefix(self, key: bytes) -> bytes:
        """Get prefix"""
        MAX_VALUE = 255
        end = list(key)
        i = len(end) - 1
        while i >= 0:
            if end[i] < MAX_VALUE:
                end[i] = (end[i] + 1) % 256
                del end[i + 1 :]
                return bytes(end)
            i -= 1
        return bytes([0])

    async def propose(self, req: RequestWithToken, use_fast_path: bool) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Send request using fast path.
        """
        cmd = Command(request=req)

        if use_fast_path:
            res = await self.curp_client.propose(cmd, True)
            return res
        else:
            res = await self.curp_client.propose(cmd, False)
            if res[1] is None:
                msg = "syncResp is always Some when useFastPath is false"
                raise Exception(msg)
            return res

    async def wait_delete(self, pfx: str, my_rev: int) -> None:
        """
        Wait until last key deleted.
        """
        rev = my_rev - 1
        while True:
            range_end = self.get_prefix(pfx.encode())
            get_req = RangeRequest(
                key=pfx.encode(),
                range_end=range_end,
                sort_order=RangeRequest.SortOrder.DESCEND,
                sort_target=RangeRequest.SortTarget.CREATE,
                max_create_revision=rev,
            )
            req = RequestWithToken(token=self.token, range_request=get_req)

            er, _ = await self.propose(req, False)
            range_res = er.range_response

            last_key: bytes = b""
            if len(range_res.kvs) > 0:
                last_key = range_res.kvs[0].key
            else:
                return

            reps, watcher_id = self.watch_client.watch(WatchCreateRequest(key=last_key))
            async for res in reps:
                watch_id = res.watch_id
                f = False
                for e in res.events:
                    if e.type == Event.DELETE:
                        self.watch_client.cancel(watcher_id, watch_id)
                        f = True
                        break
                if f:
                    break

    async def delete_key(self, key: bytes) -> ResponseHeader:
        """
        Delete key.
        """
        del_req = DeleteRangeRequest(key=key, range_end=b"\0")
        req = RequestWithToken(token=self.token, delete_range_request=del_req)

        er, _ = await self.propose(req, True)
        del_res = er.delete_range_response
        return del_res.header
