from typing import Dict, TYPE_CHECKING
import time
import asyncio

from google.protobuf.json_format import ParseDict
from ..log import logger
from ..protobuf.messages_pb2 import (
    ClusterState,
    ClusterMessageType,
    ClusterSignalHotReload,
)

from .state_handler import StateHandler
from .state_result import StateResult
from .announce_state_handler import AnnounceInstanceStateHandler

from ..instance import ThisInstance
from ..queued import IncomingMessage


class SignalHotReloadStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance, on_hot_reload):
        self._hot_reload_timestamp = time.time()
        self._on_hot_reload = on_hot_reload
        super().__init__(
            instance,
            ClusterState.INITIALIZE,
            ClusterMessageType.SIGNAL_HOT_RELOAD,
        )

    async def handle_state(self, current_state: int) -> StateResult:
        hot_reload = ClusterSignalHotReload()
        hot_reload.header.type = ClusterMessageType.SIGNAL_HOT_RELOAD
        hot_reload.timestamp = str(time.time())

        logger.info(
            "Hot reload signal sent [timestamp=%s]", hot_reload.timestamp
        )
        self._instance.cluster.udp_message_handler.send_no_wait(hot_reload)

        await asyncio.sleep(0)
        return StateResult(
            current_state,
            self,
            ClusterState.POPULATING,
            AnnounceInstanceStateHandler(self._instance),
        )

    async def handle_message(
        self, current_state: int, incoming_message: IncomingMessage
    ) -> StateResult:
        signal_hot_reload = ParseDict(
            incoming_message.message, ClusterSignalHotReload()
        )
        timestamp = float(signal_hot_reload.timestamp)
        logger.info("Hot reload signal received [timestamp=%s]", timestamp)

        if timestamp > self._hot_reload_timestamp + 5:
            logger.info("Hot reload triggered - shutting down instance")
            if self._on_hot_reload:
                self._on_hot_reload()
