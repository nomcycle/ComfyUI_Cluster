import requests
import json
from datetime import datetime

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState,
    ClusterMessageType,
    ClusterDistributePrompt,
)

from .state_handler import StateHandler
from .state_result import StateResult

from ..instance import ThisInstance
from ..env_vars import EnvVars
from ..queued import IncomingBuffer, IncomingMessage
from ..expected_msg import EXPECT_DISTRIBUTE_PROMPT_MSG_KEY


class IdleStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):
        super().__init__(
            instance,
            ClusterState.IDLE,
            ClusterMessageType.SIGNAL_IDLE
            | ClusterMessageType.DISTRIBUTE_PROMPT,
        )
        self._received_distribute_prompt_msg: bool = False

    async def handle_state(self, current_state: int) -> StateResult | None:
        if self._received_distribute_prompt_msg:
            return

        result = await self._instance.cluster.udp_message_handler.await_expected_message_thread_safe(
            EXPECT_DISTRIBUTE_PROMPT_MSG_KEY
        )
        if not result.success or not result.data:
            raise Exception("Failed to receive fanout tensor metadata")

        incoming_message: IncomingMessage = result.data
        distribute_prompt = ParseDict(
            incoming_message.message, ClusterDistributePrompt()
        )

        prompt_json = json.loads(distribute_prompt.prompt)
        json_data = {
            "prompt": prompt_json["output"],
            "extra_data": {"extra_pnginfo": prompt_json["workflow"]},
            "client_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        }

        url = f"http://localhost:{EnvVars.get_comfy_port()}/prompt"
        try:
            response = requests.post(url, json=json_data)
            response.raise_for_status()
            logger.info("Successfully posted prompt to local ComfyUI instance")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error posting prompt: {str(e)}")

        self._received_distribute_prompt_msg = True

    async def handle_message(
        self, current_state: int, incoming_message: IncomingMessage
    ) -> StateResult | None:
        return

    async def handle_buffer(
        self, current_state: int, incoming_buffer: IncomingBuffer
    ):
        return
