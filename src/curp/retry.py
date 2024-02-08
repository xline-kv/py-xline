"""
Retry client
"""

from __future__ import annotations
import logging
import time
from enum import Enum
from datetime import timedelta
from dataclasses import dataclass
from src.curp.unary import Unary
from src.rpc.type import CurpError
from api.curp.curp_command_pb2 import CurpError as _CurpError
from api.xline.xline_command_pb2 import Command, CommandResponse, SyncResponse


class BackoffConfig(Enum):
    """
    Backoff config
    """

    # A fixed delay backoff
    FIXED = 1
    # A exponential delay backoff
    EXPONENTIAL = 2


@dataclass
class RetryConfig:
    """
    Retry config to control the retry policy
    """

    # Backoff config
    backoff: BackoffConfig
    # Initial delay
    delay: timedelta
    # Control the max delay of exponential
    max_delay: timedelta | None
    # Retry count
    count: int

    @classmethod
    def new_fixed(cls, delay: timedelta, count: int) -> RetryConfig:
        """
        Backoff config
        """
        if count <= 0:
            raise RuntimeError("retry count should be larger than 0")
        return cls(BackoffConfig.FIXED, delay, None, count)

    @classmethod
    def new_exponential(cls, delay: timedelta, max_delay: timedelta, count: int) -> RetryConfig:
        """
        Backoff config
        """
        if count <= 0:
            raise RuntimeError("retry count should be larger than 0")
        return cls(BackoffConfig.EXPONENTIAL, delay, max_delay, count)

    def init_backoff(self) -> Backoff:
        """
        Create a backoff process
        """
        return Backoff(self, self.delay, self.count)


@dataclass
class Backoff:
    """
    Backoff tool
    """

    # The retry config
    config: RetryConfig
    # Current delay
    cur_delay: timedelta
    # Total RPC count
    count: int

    def next_delay(self) -> timedelta | None:
        """
        Get the next delay duration, None means the end.
        """
        if self.count == 0:
            return None
        self.count -= 1
        cur = self.cur_delay
        if self.config.backoff == BackoffConfig.EXPONENTIAL:
            if self.config.max_delay is None:
                raise RuntimeError("max_delay must be set, if use exponential backoff")
            self.cur_delay = min(cur * 2, self.config.max_delay)
        return cur


@dataclass
class Retry:
    """
    The retry client automatically retry the requests of the inner client api
    """

    # Inner client
    inner: Unary
    # Retry config
    config: RetryConfig

    async def propose(self, cmd: Command, use_fast_path: bool) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
        requests (event the requests are commutative).
        """
        backoff = self.config.init_backoff()
        last_err: CurpError | None = None

        while True:
            delay = backoff.next_delay()
            if delay is None:
                break

            propose_id = self.inner.gen_propose_id()

            try:
                return await self.inner.repeatable_propose(propose_id, cmd, use_fast_path)
            except CurpError as e:
                # some errors that should not retry
                if (
                    e.inner.HasField("Duplicated")
                    or e.inner.HasField("ShuttingDown")
                    or e.inner.HasField("InvalidConfig")
                    or e.inner.HasField("NodeNotExists")
                    or e.inner.HasField("NodeAlreadyExists")
                    or e.inner.HasField("LearnerNotCatchUp")
                ):
                    raise e
                # some errors that could have a retry
                if (
                    e.inner.HasField("ExpiredClientId")
                    or e.inner.HasField("KeyConflict")
                    or e.inner.HasField("Internal")
                ):
                    pass
                # update leader state if we got a rpc transport error
                if e.inner.HasField("RpcTransport"):
                    try:
                        await self.inner.fetch_leader_id()
                    except CurpError as err:
                        logging.warning("fetch leader failed, error %s", err)
                # update the cluster state if got WrongClusterVersion
                if e.inner.HasField("WrongClusterVersion"):
                    # the inner client should automatically update cluster state when fetch_cluster
                    try:
                        await self.inner.fetch_cluster()
                    except CurpError as err:
                        logging.warning("fetch leader failed, error %s", err)
                # update the leader state if got Redirect
                if e.inner.HasField("redirect"):
                    leader_id = e.inner.redirect.leader_id
                    term = e.inner.redirect.term
                    self.inner.update_leader(leader_id, term)

                last_err = e
                time.sleep(delay)

        raise CurpError(_CurpError(Internal=f"request timeout, last error: {last_err}"))
