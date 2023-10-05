"""Kv Client"""

import uuid
from typing import Optional, Tuple
from client.protocol import ProtocolClient as CurpClient, ProposeError
from api.xline.xline_command_pb2 import Command, RequestWithToken, KeyRange
from api.xline.rpc_pb2 import (
    RangeRequest,
    RangeResponse,
    PutRequest,
    PutResponse,
    DeleteRangeRequest,
    DeleteRangeResponse,
    TxnRequest,
    TxnResponse,
    CompactionRequest,
    CompactionResponse,
)


class KvClient:
    """
    Client for KV operations.

    Attributes:
        name: Name of the kv client, which will be used in CURP propose id generation.
        curp_client: The client running the CURP protocol, communicate with all servers.
        token: The auth token.
    """

    name: str
    curp_client: CurpClient
    token: Optional[str]

    def __init__(self, name: str, curp_client: CurpClient, token: Optional[str]) -> None:
        self.name = name
        self.curp_client = curp_client
        self.token = token

    def range(self, req: RangeRequest) -> Tuple[Optional[RangeResponse], Optional[ProposeError]]:
        """
        Get a range of keys from the store.
        """
        key_ranges = [KeyRange(key=req.key, range_end=req.range_end)]
        propose_id = generate_propose_id(self.name)
        request_with_token = RequestWithToken(
            range_request=req,
            token=self.token,
        )
        cmd = Command(
            keys=key_ranges,
            request=request_with_token,
            propose_id=propose_id,
        )
        propose_res, err = self.curp_client.propose(cmd, True)
        if propose_res is not None:
            cmd_res, _ = propose_res
            return cmd_res.range_response, None
        else:
            return None, err

    def put(self, req: PutRequest) -> Tuple[Optional[PutResponse], Optional[ProposeError]]:
        """
        Put a key-value into the store.
        """
        key_ranges = [KeyRange(key=req.key, range_end=req.key)]
        propose_id = generate_propose_id(self.name)
        request_with_token = RequestWithToken(
            put_request=req,
            token=self.token,
        )
        cmd = Command(
            keys=key_ranges,
            request=request_with_token,
            propose_id=propose_id,
        )
        propose_res, err = self.curp_client.propose(cmd, True)
        if propose_res is not None:
            cmd_res, _ = propose_res
            return cmd_res.put_response, None
        else:
            return None, err

    def delete(self, req: DeleteRangeRequest) -> Tuple[Optional[DeleteRangeResponse], Optional[ProposeError]]:
        """
        Delete a range of keys from the store.
        """
        key_ranges = [KeyRange(key=req.key, range_end=req.key)]
        propose_id = generate_propose_id(self.name)

        request_with_token = RequestWithToken(
            delete_range_request=req,
            token=self.token,
        )
        cmd = Command(
            keys=key_ranges,
            request=request_with_token,
            propose_id=propose_id,
        )
        propose_res, err = self.curp_client.propose(cmd, True)
        if propose_res is not None:
            cmd_res, _ = propose_res
            return cmd_res.delete_range_response, None
        else:
            return None, err

    def txn(self, req: TxnRequest) -> Tuple[Optional[TxnResponse], Optional[ProposeError]]:
        """
        Creates a transaction, which can provide serializable writes.
        """

        key_ranges = []
        for cmp in req.compare:
            key_ranges.append(KeyRange(key=cmp.key, range_end=cmp.range_end))

        propose_id = generate_propose_id(self.name)
        request_with_token = RequestWithToken(
            txn_request=req,
            token=self.token,
        )
        cmd = Command(
            keys=key_ranges,
            request=request_with_token,
            propose_id=propose_id,
        )
        propose_res, err = self.curp_client.propose(cmd, False)
        if propose_res is not None:
            cmd_res, _ = propose_res
            return cmd_res.txn_response, None
        else:
            return None, err

    def compact(self, req: CompactionRequest) -> Tuple[Optional[CompactionResponse], Optional[ProposeError]]:
        """
        Compacts the key-value store up to a given revision.
        """
        use_fast_path = req.physical
        propose_id = generate_propose_id(self.name)
        request_with_token = RequestWithToken(
            compaction_request=req,
            token=self.token,
        )
        cmd = Command(
            request=request_with_token,
            propose_id=propose_id,
        )

        if use_fast_path:
            propose_res, err = self.curp_client.propose(cmd, True)
            if propose_res is not None:
                cmd_res, _ = propose_res
                return cmd_res.compaction_response, None
            else:
                return None, err
        else:
            propose_res, err = self.curp_client.propose(cmd, False)
            if propose_res is not None:
                cmd_res, sync_res = propose_res
                if sync_res is not None:
                    cmd_res.compaction_response.header.revision = sync_res.revision
                else:
                    return None, "sync_res is always Some when use_fast_path is false"
                return cmd_res.compaction_response, None
            else:
                return None, err


def generate_propose_id(prefix: str) -> str:
    """Generate propose id with the given prefix"""
    propose_id = f"{prefix}-{uuid.uuid4()}"
    return propose_id
