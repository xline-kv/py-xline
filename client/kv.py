"""Kv Client"""

from typing import Optional, Literal
from client.protocol import ProtocolClient as CurpClient
from client.txn import Txn
from api.xline.xline_command_pb2 import Command, RequestWithToken, KeyRange
from api.xline.rpc_pb2 import (
    RangeRequest,
    RangeResponse as _RangeResponse,
    PutRequest,
    PutResponse as _PutResponse,
    DeleteRangeRequest,
    DeleteRangeResponse as _DeleteRangeResponse,
    CompactionRequest,
    CompactionResponse as _CompactionResponse,
)

RangeResponse = _RangeResponse
PutResponse = _PutResponse
DeleteRangeResponse = _DeleteRangeResponse
CompactionResponse = _CompactionResponse


class KvClient:
    """
    Client for KV operations.

    Attributes:
        name: Name of the kv client, which will be used in CURP propose id generation.
        curp_client: The client running the CURP protocol, communicate with all servers.
        token: The auth token.
    """

    curp_client: CurpClient
    token: Optional[str]

    def __init__(self, curp_client: CurpClient, token: Optional[str]) -> None:
        self.curp_client = curp_client
        self.token = token

    async def range(
        self,
        key: bytes,
        range_end: bytes | None = None,
        limit: int | None = None,
        revision: int | None = None,
        sort_order: Literal["none", "ascend", "descend"] | None = None,
        sort_target: Literal["key", "version", "create", "mod", "value"] | None = None,
        serializable: bool = False,
        keys_only: bool = False,
        count_only: bool = False,
        min_mod_revision: int | None = None,
        max_mod_revision: int | None = None,
        min_create_revision: int | None = None,
        max_create_revision: int | None = None,
    ) -> RangeResponse:
        """
        Get a range of keys from the store.
        """
        req = RangeRequest(
            key=key,
            range_end=range_end,
            limit=limit,
            revision=revision,
            sort_order=sort_order,
            sort_target=sort_target,
            serializable=serializable,
            keys_only=keys_only,
            count_only=count_only,
            min_mod_revision=min_mod_revision,
            max_mod_revision=max_mod_revision,
            min_create_revision=min_create_revision,
            max_create_revision=max_create_revision,
        )
        key_ranges = [KeyRange(key=key, range_end=range_end)]
        request_with_token = RequestWithToken(
            range_request=req,
            token=self.token,
        )
        cmd = Command(
            keys=key_ranges,
            request=request_with_token,
        )
        er, _ = await self.curp_client.propose(cmd, True)
        return er.range_response

    async def put(
        self,
        key: bytes,
        value: bytes,
        lease: int | None = None,
        prev_kv: bool = False,
        ignore_value: bool = False,
        ignore_lease: bool = False,
    ) -> PutResponse:
        """
        Put a key-value into the store.
        """
        req = PutRequest(
            key=key, value=value, lease=lease, prev_kv=prev_kv, ignore_value=ignore_value, ignore_lease=ignore_lease
        )
        key_ranges = [KeyRange(key=key, range_end=key)]
        request_with_token = RequestWithToken(
            put_request=req,
            token=self.token,
        )
        cmd = Command(
            keys=key_ranges,
            request=request_with_token,
        )
        er, _ = await self.curp_client.propose(cmd, True)
        return er.put_response

    async def delete(self, key: bytes, range_end: bytes | None = None, prev_kv: bool = False) -> DeleteRangeResponse:
        """
        Delete a range of keys from the store.
        """
        req = DeleteRangeRequest(
            key=key,
            range_end=range_end,
            prev_kv=prev_kv,
        )
        key_ranges = [KeyRange(key=key, range_end=range_end)]

        request_with_token = RequestWithToken(
            delete_range_request=req,
            token=self.token,
        )
        cmd = Command(
            keys=key_ranges,
            request=request_with_token,
        )
        er, _ = await self.curp_client.propose(cmd, True)
        return er.delete_range_response

    def txn(self) -> Txn:
        """
        Creates a transaction, which can provide serializable writes.
        """

        return Txn(
            self.curp_client,
            self.token,
        )

    async def compact(self, revision: int, physical: bool = False) -> CompactionResponse:
        """
        Compacts the key-value store up to a given revision.
        """
        req = CompactionRequest(
            revision=revision,
            physical=physical,
        )
        use_fast_path = physical
        request_with_token = RequestWithToken(
            compaction_request=req,
            token=self.token,
        )
        cmd = Command(
            request=request_with_token,
        )

        if use_fast_path:
            er, asr = await self.curp_client.propose(cmd, True)
            return er.compaction_response
        else:
            er, asr = await self.curp_client.propose(cmd, False)
            if asr is None:
                msg = "sync_res is always Some when use_fast_path is false"
                raise Exception(msg)
            return er.compaction_response
