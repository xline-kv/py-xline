"""
Rpc type
"""

from __future__ import annotations
from curp_command_pb2 import (
    CurpError as _CurpError,
    ProposeResponse as _ProposeResponse,
    WaitSyncedResponse as _WaitSyncedResponse,
)
from api.xline.xline_command_pb2 import CommandResponse, SyncResponse
from api.xline.xline_error_pb2 import ExecuteError
import grpc
from grpc_status import rpc_status


class ProposeResponse:
    """
    Propose response
    """

    __inner: _ProposeResponse

    def __init__(self, res: _ProposeResponse) -> None:
        self.__inner = res

    def deserialize(self) -> tuple[CommandResponse | None, ExecuteError | None]:
        """
        Deserialize result in response
        """
        res = self.__inner
        er = CommandResponse()
        err = ExecuteError()

        if not res.result.HasField("result"):
            return None, None
        if res.result.HasField("ok"):
            er.ParseFromString(res.result.ok)
            return er, None
        if res.result.HasField("error"):
            err.ParseFromString(res.result.error)
            return None, err
        raise Exception("unknown error")


class WaitSyncedResponse:
    """
    Wait synced response
    """

    __inner: _WaitSyncedResponse

    def __init__(self, res: _WaitSyncedResponse) -> None:
        self.__inner = res

    def deserialize(self) -> tuple[tuple[CommandResponse, SyncResponse] | None, ExecuteError | None]:
        """
        according to the above methods, we can only get the following response union
        ER: Some(OK), ASR: Some(OK)  <-  WaitSyncedResponse::new_success
        ER: Some(Err), ASR: None     <-  WaitSyncedResponse::new_er_error
        ER: Some(OK), ASR: Some(Err) <- WaitSyncedResponse::new_asr_error
        """
        res = self.__inner
        if res.exe_result.HasField("ok") and res.after_sync_result.HasField("ok"):
            er = CommandResponse()
            asr = SyncResponse()
            er.ParseFromString(res.exe_result.ok)
            asr.ParseFromString(res.after_sync_result.ok)
            return (er, asr), None
        if res.exe_result.HasField("error") and not res.HasField("after_sync_result"):
            err = ExecuteError()
            err.ParseFromString(res.exe_result.error)
            return None, err
        if res.exe_result.HasField("ok") and res.after_sync_result.HasField("error"):
            err = ExecuteError()
            err.ParseFromString(res.after_sync_result.error)
            return None, err
        raise Exception("got unexpected WaitSyncedResponse")


CurpErrorPriority = int
LOW = 0
HIGH = 1


class CurpError(RuntimeError):
    """
    Curp error
    """

    inner: _CurpError
    comes: str

    def __init__(self, err: _CurpError, comes: str | None = None) -> None:
        self.inner = err
        self.comes = comes

    @classmethod
    def build_from_rpc_error(cls, err: grpc.RpcError, comes: str | None = None) -> CurpError:
        """
        Build curp error from rpc error
        """
        status: rpc_status._Status = rpc_status.from_call(err)
        detail = status.details[0]
        curp_err = _CurpError()
        curp_err.ParseFromString(detail.value)
        return cls(curp_err, comes)

    def should_about_fast_round(self) -> bool:
        """
        Whether to abort fast round early
        """
        if (
            self.inner.HasField("Duplicated")
            or self.inner.HasField("ShuttingDown")
            or self.inner.HasField("InvalidConfig")
            or self.inner.HasField("NodeAlreadyExists")
            or self.inner.HasField("NodeNotExists")
            or self.inner.HasField("LearnerNotCatchUp")
            or self.inner.HasField("ExpiredClientId")
            or self.inner.HasField("redirect")
        ):
            return True
        return False

    def should_about_slow_round(self) -> bool:
        """
        Whether to abort slow round early
        """
        if (
            self.inner.HasField("ShuttingDown")
            or self.inner.HasField("InvalidConfig")
            or self.inner.HasField("NodeAlreadyExists")
            or self.inner.HasField("NodeNotExists")
            or self.inner.HasField("LearnerNotCatchUp")
            or self.inner.HasField("ExpiredClientId")
            or self.inner.HasField("redirect")
            or self.inner.HasField("WrongClusterVersion")
        ):
            return True
        return False

    def priority(self) -> CurpErrorPriority:
        """
        Get the priority of the error
        """
        if (
            self.inner.HasField("Duplicated")
            or self.inner.HasField("ShuttingDown")
            or self.inner.HasField("InvalidConfig")
            or self.inner.HasField("NodeAlreadyExists")
            or self.inner.HasField("NodeNotExists")
            or self.inner.HasField("LearnerNotCatchUp")
            or self.inner.HasField("ExpiredClientId")
            or self.inner.HasField("redirect")
            or self.inner.HasField("WrongClusterVersion")
        ):
            return HIGH
        if self.inner.HasField("RpcTransport") or self.inner.HasField("Internal") or self.inner.HasField("KeyConflict"):
            return LOW
