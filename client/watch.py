"""Watch Client"""

import uuid
import asyncio
from grpc import Channel
from grpc.aio import StreamStreamCall
from api.xline.rpc_pb2_grpc import WatchStub
from api.xline.rpc_pb2 import (
    WatchRequest,
    WatchCreateRequest,
    WatchCancelRequest,
)


class Watcher:
    """
    Watcher for Watch operations.

    Attributes:
        watch_client: The watch RPC client, only communicate with one server at a time.
        is_cancel: Whether the watcher is canceled.
        watch_id: The ID of the watcher.
    """

    watch_client: WatchStub
    is_cancel: bool
    watch_id: int

    def __init__(self, watch_client: WatchStub) -> None:
        self.watch_client = watch_client
        self.is_cancel = False
        self.watch_id = -1

    def watch(self, req: WatchCreateRequest) -> StreamStreamCall:
        """
        Watches for events happening or that have happened. Both input and output
        are streams; the input stream is for creating and canceling watcher and the output
        stream sends events. The entire event history can be watched starting from the
        last compaction revision.
        """

        async def watch():
            yield WatchRequest(create_request=req)

            while not self.is_cancel:
                await asyncio.sleep(0.5)

            yield WatchRequest(cancel_request=WatchCancelRequest(watch_id=self.watch_id))

        res = self.watch_client.Watch(watch())
        return res

    def cancel(self, watch_id: int) -> None:
        """
        Cancel the Watcher
        """
        self.is_cancel = True
        self.watch_id = watch_id


class WatchClient:
    """
    Client for Watch operations.

    Attributes:
        watch_client: The watch RPC client, only communicate with one server at a time.
        watchers: The list of watchers.
    """

    watch_client: WatchStub
    watchers: dict[str, Watcher]

    def __init__(self, channel: Channel) -> None:
        self.watch_client = WatchStub(channel=channel)
        self.watchers = {}

    def watch(self, req: WatchCreateRequest) -> tuple[StreamStreamCall, str]:
        """
        Create Watcher to watch
        """
        watcher = Watcher(self.watch_client)

        watch_id = str(uuid.uuid4())
        self.watchers[watch_id] = watcher

        res = watcher.watch(req)
        return res, watch_id

    def cancel(self, watcher_id: str, watch_id: int) -> None:
        """
        Cancel the Watcher
        """
        if watcher_id in self.watchers:
            self.watchers[watcher_id].cancel(watch_id)
            del self.watchers[watcher_id]
