"""
Unary client
"""

from __future__ import annotations
import asyncio
import grpc
import logging
import threading
import random
from api.curp.curp_command_pb2 import (
    ProposeId,
    FetchClusterRequest,
    FetchClusterResponse,
    ProposeRequest,
    WaitSyncedRequest,
    CurpError as _CurpError,
)
from api.xline.xline_command_pb2 import Command, CommandResponse, SyncResponse
from api.xline.xline_error_pb2 import ExecuteError
from src.rpc.connect import Connect
from src.rpc.type import CurpError, ProposeResponse, WaitSyncedResponse
from google.protobuf.empty_pb2 import Empty


class State:
    """
    Client state
    """

    # Leader id. At the beginning, we may not know who the leader is.
    leader: int | None
    # Term, initialize to 0, calibrated by the server.
    term: int
    # Cluster version, initialize to 0, calibrated by the server.
    cluster_version: int
    # Members' connect
    connects: dict[int, Connect]

    def __init__(
        self,
        leader: int | None,
        term: int,
        cluster_version: int,
        connects: dict[int, Connect],
    ) -> None:
        self.leader = leader
        self.term = term
        self.cluster_version = cluster_version
        self.connects = connects


class UnaryConfig:
    """
    The unary client config
    """

    # The rpc timeout of a propose request
    propose_timeout: float
    # The rpc timeout of a 2-RTT request, usually takes longer than propose timeout
    # The recommended the values is within (propose_timeout, 2 * propose_timeout].
    wait_synced_timeout: float

    def __init__(
        self,
        propose_timeout: float,
        wait_synced_timeout: float,
    ) -> None:
        self.propose_timeout = propose_timeout
        self.wait_synced_timeout = wait_synced_timeout


class UnaryBuilder:
    """
    Unary builder
    """

    # All members (required)
    __all_members: dict[int, list[str]]
    # Unary config (required)
    __config: UnaryConfig
    # Leader state (optional)
    __leader_state: tuple[int, int] | None
    # Cluster version (optional)
    __cluster_version: int | None

    def __init__(self, all_members: dict[int, list[str]], config: UnaryConfig) -> None:
        """
        Create unary builder
        """
        self.__all_members = all_members
        self.__config = config
        self.__leader_state = None
        self.__cluster_version = None

    def set_leader_state(self, server_id: int, term: int) -> UnaryBuilder:
        """
        Set the leader state (optional)
        """
        self.__leader_state = (server_id, term)
        return self

    def set_cluster_version(self, cluster_version: int) -> UnaryBuilder:
        """
        Set the cluster version (optional)
        """
        self.__cluster_version = cluster_version
        return self

    def build_with_connects(self, connects: dict[int, Connect]) -> Unary:
        """
        Inner build
        """
        leader_id = 0
        term = 1
        if self.__leader_state:
            leader_id = self.__leader_state[0]
            term = self.__leader_state[1]
        cluster_version = 0
        if self.__cluster_version:
            cluster_version = self.__cluster_version
        state = State(leader_id, term, cluster_version, connects)
        return Unary(state, self.__config)

    def build(self) -> Unary:
        """
        Build the unary client
        """
        conns = Connect.connects(self.__all_members)
        return self.build_with_connects(conns)


class Unary:
    """
    The unary client
    """

    # Client state
    __state: State
    # Unary config
    __config: UnaryConfig
    # The lock
    __lock: threading.Lock

    def __init__(self, state: State, config: UnaryConfig) -> None:
        self.__state = state
        self.__config = config
        self.__lock = threading.Lock()

    async def fetch_leader_id(self) -> int:
        """
        Fetch leader id
        """
        res = await self.fetch_cluster()
        return res.leader_id

    async def fetch_cluster(self) -> FetchClusterResponse:
        """
        Send fetch cluster requests to all servers
        """
        timeout = self.__config.wait_synced_timeout

        futures: list[asyncio.Future[FetchClusterResponse]] = []
        for _, conn in self.__state.connects.items():
            future = conn.fetch_cluster(FetchClusterRequest(linearizable=True), timeout)
            futures.append(future)

        quorum: int = self.quorum(len(self.__state.connects))
        max_term: int = 0
        resp: FetchClusterResponse | None = None
        ok_cnt: int = 0
        curp_err: CurpError | None = None

        for future in asyncio.as_completed(futures):
            try:
                res: FetchClusterResponse = await future
            except CurpError as e:
                err = CurpError(e, "fetch cluster")
                if err.should_about_fast_round():
                    raise err from e
                if curp_err is not None:
                    old_err: CurpError = curp_err
                    if old_err.priority() <= err.priority():
                        curp_err = err
                    else:
                        curp_err = err

            # Ignore the response of a node that doesn't know who the leader is.
            if res.HasField("leader_id"):
                if max_term < res.term:
                    max_term = res.term
                    if len(res.members) != 0:
                        resp = res
                    # reset ok count to 1
                    ok_cnt = 1
                elif max_term == res.term:
                    if len(res.members) != 0:
                        resp = res
                    ok_cnt += 1
                else:
                    pass

            # first check quorum
            if ok_cnt >= quorum:
                # then check if we got the response
                if resp is not None:
                    logging.debug("fetch cluster succeeded, result: %s", resp)
                    try:
                        self.check_and_update(resp)
                    except CurpError as e:
                        logging.warning("update to a new cluster state failed, error %s", e)
                    return resp
                logging.debug("fetch cluster quorum ok, but members are empty")

        if curp_err is not None:
            raise curp_err

        raise CurpError(_CurpError(RpcTransport=Empty()))
    
    def update_leader(self, leader_id: int | None, term: int) -> bool:
        """
        Update leader
        """
        return self.check_and_update_leader(self.__state, leader_id, term)

    def check_and_update(self, res: FetchClusterResponse) -> None:
        """
        Update client state based on `FetchClusterResponse`
        """
        self.__lock.locked()

        if not self.check_and_update_leader(self.__state, res.leader_id, res.term):
            return
        if self.__state.cluster_version == res.cluster_version:
            logging.debug("ignore cluster version%s from server", res.cluster_version)
            return

        logging.debug("client cluster version updated to %s", res.cluster_version)
        self.__state.cluster_version = res.cluster_version

        new_members: dict[int, list[str]] = {}
        for member in res.members:
            new_members[member.id] = member.addrs

        old_ids = set(self.__state.connects.keys())
        new_ids = set(new_members.keys())

        diffs = old_ids - new_ids
        sames = old_ids & new_ids

        for diff in diffs:
            if diff in new_members:
                if diff not in self.__state.connects:
                    raise CurpError(_CurpError(Internal="diff must in old member addrs"))
                logging.debug("client connects to a new server%s, address%s", diff, new_members[diff])
                conn = Connect.connect(diff, new_members[diff])
                self.__state.connects[diff] = conn
            else:
                logging.debug("client removes old server%s", diff)
                del self.__state.connects[diff]
        for same in sames:
            if same in self.__state.connects:
                conn = self.__state.connects[same]
                if same in new_members:
                    conn.update_addrs(new_members[same])
                else:
                    raise CurpError(_CurpError(Internal="same must in new member addrs"))
            else:
                raise CurpError(_CurpError(Internal="some must in old member addrs"))

        return

    def check_and_update_leader(self, state: State, leader_id: int | None, term: int) -> bool:
        """
        Update leader
        """
        if state.term < term:
            # reset term only when the resp has leader id to prevent:
            # If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
            # But if the client learns about the new term and updates its term to it, it will never get the true leader.
            if leader_id is not None:
                new_leader_id = leader_id
                logging.debug("client term updates to %s", term)
                logging.debug("client leader id updates to %s", new_leader_id)
                state.term = term
                state.leader = new_leader_id
        elif state.term == term:
            if leader_id is not None:
                new_leader_id = leader_id
                if state.leader is None:
                    logging.debug("client leader id updates to %s", new_leader_id)
                    state.leader = new_leader_id
                if state.leader != new_leader_id:
                    raise CurpError(_CurpError(Internal="there should never be two leader in one term"))
        else:
            logging.debug("ignore old term%s from server", term)
            return False

        return True

    async def propose(self, cmd: Command, use_fast_path: bool = True) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
        requests (event the requests are commutative).
        """
        propose_id = self.gen_propose_id()
        return await self.repeatable_propose(propose_id, cmd, use_fast_path)

    async def repeatable_propose(
        self, propose_id: ProposeId, cmd: Command, use_fast_path: bool = True
    ) -> tuple[CommandResponse, SyncResponse | None]:
        """
        Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
        requests (event the requests are commutative).
        """
        fast_round = self.fast_round(propose_id, cmd)
        slow_round = self.slow_round(propose_id)

        prev_err = None

        if use_fast_path:
            for future in asyncio.as_completed([fast_round, slow_round]):
                try:
                    exe_ret, _ = await future
                    if isinstance(exe_ret, CommandResponse):
                        return (exe_ret, None)
                    return exe_ret
                except CurpError as e:
                    if e.comes == "fast_round":
                        if e.should_about_slow_round():
                            raise e
                        if prev_err is None:
                            prev_err = e
                            continue
                        if prev_err.priority() < e.priority():
                            prev_err = e
                            continue
                    if e.comes == "slow_round":
                        if e.should_about_fast_round():
                            raise e
                        if prev_err is None:
                            prev_err = e
                            continue
                        if prev_err.priority() < e.priority():
                            prev_err = e
                            continue
        else:
            try:
                _, (res, _) = await asyncio.gather(fast_round, slow_round)
                if res is not None:
                    return res
            except CurpError as e:
                raise e

        raise prev_err

    async def fast_round(
        self,
        propose_id: ProposeId,
        cmd: Command,
    ) -> tuple[CommandResponse | None, ExecuteError | None]:
        """
        Send proposal to all servers
        """
        req = ProposeRequest(
            propose_id=propose_id, command=cmd.SerializePartialToString(), cluster_version=self.__state.cluster_version
        )
        timeout = self.__config.propose_timeout

        futures: list[asyncio.Future[ProposeResponse]] = []
        for _, conn in self.__state.connects.items():
            future = conn.propose(req, timeout)
            futures.append(future)

        super_quorum = self.super_quorum(len(self.__state.connects))
        exe_ret: CommandResponse | None = None
        curp_err = None
        ok_cnt = 0

        for future in asyncio.as_completed(futures):
            try:
                r = await future
                ok_cnt += 1
                er, err = ProposeResponse(r).deserialize()
                if er is not None:
                    if err is not None:
                        return None, err
                    if exe_ret is not None:
                        raise CurpError(_CurpError(Internal="should not set exe result twice"))
                    if er is not None:
                        exe_ret = er
            except grpc.RpcError as e:
                err = CurpError.build_from_rpc_error(e, comes="fast_round")
                if err.should_about_fast_round():
                    raise err from e
                if curp_err is not None:
                    old_err = curp_err
                    if old_err.priority() < err.priority():
                        curp_err = err
                else:
                    curp_err = err

            if ok_cnt >= super_quorum:
                if exe_ret is not None:
                    return exe_ret, None

        if curp_err is not None:
            raise curp_err

        raise CurpError(_CurpError(WrongClusterVersion=Empty()))

    async def slow_round(
        self, propose_id: ProposeId
    ) -> tuple[tuple[CommandResponse, SyncResponse] | None, ExecuteError | None]:
        """
        Wait synced result from server
        """
        timeout = self.__config.wait_synced_timeout
        req = WaitSyncedRequest(propose_id=propose_id, cluster_version=self.__state.cluster_version)

        cached_leader = self.__state.leader
        leader_id = 0
        if cached_leader is not None:
            leader_id = cached_leader
        else:
            leader_id = await self.fetch_leader_id()

        conn = self.__state.connects[leader_id]

        try:
            res = await conn.wait_synced(req, timeout)
            return WaitSyncedResponse(res).deserialize()
        except grpc.RpcError as e:
            err = CurpError.build_from_rpc_error(e, comes="slow_round")
            raise err from e

    def gen_propose_id(self) -> ProposeId:
        """
        Generate a propose id
        """
        client_id = self.get_client_id()
        seq_num = self.new_seq_num()
        return ProposeId(client_id=client_id, seq_num=seq_num)

    def get_client_id(self) -> int:
        """
        Get the client id
        TODO: grant a client id from server
        """
        return random.randint(0, 2**64 - 1)

    def new_seq_num(self) -> int:
        """
        New a seq num and record it
        TODO: implement request tracker
        """
        return 0

    @staticmethod
    def super_quorum(nodes: int) -> int:
        """
        Calculate the super quorum
        Although curp can proceed with f + 1 available replicas, it needs f + 1 + (f + 1)/2 replicas
        (for superquorum of witnesses) to use 1 RTT operations. With less than superquorum replicas,
        clients must ask masters to commit operations in f + 1 replicas before returning result.(2 RTTs).
        """
        fault_tolerance = nodes // 2
        quorum = fault_tolerance + 1
        superquorum = fault_tolerance + (quorum // 2) + 1
        return superquorum

    @staticmethod
    def quorum(size: int) -> int:
        """
        Calculate the quorum
        """
        return size // 2 + 1
