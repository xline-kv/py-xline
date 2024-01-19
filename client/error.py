"""
Client Errors
"""

from api.xline.xline_error_pb2 import ExecuteError as _ExecuteError


class ExecuteError(Exception):
    """Execute error"""

    inner: _ExecuteError

    def __init__(self, err: _ExecuteError) -> None:
        self.inner = err


class ShuttingDownError(Exception):
    """Server is shutting down"""

    pass


class WrongClusterVersionError(Exception):
    """Wrong cluster version"""

    pass


class InternalError(Exception):
    """Internal Error in client"""

    pass
