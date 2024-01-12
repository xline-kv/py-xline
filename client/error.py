"""
Client Errors
"""

from api.xline.xline_error_pb2 import ExecuteError as _ExecuteError


class ExecuteError(Exception):
    """Execute error"""

    inner: _ExecuteError

    def __init__(self, err: _ExecuteError) -> None:
        self.inner = err
