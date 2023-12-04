"Transaction"

import uuid
from typing import List, Optional
from client.client import ProtocolClient as CurpClient
from api.xline.xline_command_pb2 import Command, RequestWithToken, KeyRange
from api.xline.rpc_pb2 import (
    RangeRequest as _RangeRequest,
    PutRequest as _PutRequest,
    DeleteRangeRequest as _DeleteRangeRequest,
    TxnRequest as _TxnRequest,
    Compare as _Compare,
    RequestOp as _RequestOp,
    TxnResponse as _TxnResponse,
)

RangeRequest = _RangeRequest
PutRequest = _PutRequest
DeleteRangeRequest = _DeleteRangeRequest
TxnRequest = _TxnRequest
Compare = _Compare
RequestOp = _RequestOp
TxnResponse = _TxnResponse


class Txn:
    """
    Transaction.

    Attributes:
        name: Name of the Transaction, which will be used in CURP propose id generation.
        curp_client: The client running the CURP protocol, communicate with all servers.
        token: The auth token.
    """

    name: str
    curp_client: CurpClient
    token: Optional[str]

    cmps: List[Compare]
    sus: List[RequestOp]
    fas: List[RequestOp]

    def __init__(self, name: str, curp_client: CurpClient, token: Optional[str]) -> None:
        self.name = name
        self.curp_client = curp_client
        self.token = token

    def when(self, cmps: List[Compare]):
        "compare"
        self.cmps = cmps
        return self

    def and_then(self, op: List[RequestOp]):
        "true"
        self.sus = op
        return self

    def or_else(self, op: List[RequestOp]):
        "false"
        self.fas = op
        return self

    async def commit(self) -> TxnResponse:
        "commit"
        # TODO: https://github.com/xline-kv/Xline/issues/470
        krs = []
        for cmp in self.cmps:
            krs.append(KeyRange(key=cmp.key, range_end=cmp.range_end))
        propose_id = self.generate_propose_id(self.name)
        r = TxnRequest(compare=self.cmps, success=self.sus, failure=self.fas)
        req = RequestWithToken(
            txn_request=r,
            token=self.token,
        )
        cmd = Command(
            keys=krs,
            request=req,
            propose_id=propose_id,
        )
        er, _ = await self.curp_client.propose(cmd, False)
        return er.txn_response

    @staticmethod
    def generate_propose_id(prefix: str) -> str:
        """Generate propose id with the given prefix"""
        propose_id = f"{prefix}-{uuid.uuid4()}"
        return propose_id
