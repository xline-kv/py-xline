"""
Client Errors
"""

from api.curp.curp_error_pb2 import (
    ProposeError as _ProposeError,
    CommandSyncError as _CommandSyncError,
    WaitSyncError as _WaitSyncError,
)
from api.xline.xline_error_pb2 import ExecuteError as _ExecuteError


class ResDecodeError(Exception):
    """Response decode error"""

    pass


class ProposeError(BaseException):
    """Propose error"""

    inner: _ProposeError

    def __init__(self, err: _ProposeError) -> None:
        self.inner = err


class CommandSyncError(BaseException):
    """Command sync error"""

    inner: _CommandSyncError

    def __init__(self, err: _CommandSyncError) -> None:
        self.inner = err


class WaitSyncError(BaseException):
    """Wait sync error"""

    inner: _WaitSyncError

    def __init__(self, err: _WaitSyncError) -> None:
        self.inner = err


class ExecuteError(BaseException):
    """Execute error"""

    inner: _ExecuteError

    def __init__(self, err: _ExecuteError) -> None:
        self.inner = err
