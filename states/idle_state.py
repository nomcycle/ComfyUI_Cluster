import asyncio
import requests
import json
from datetime import datetime
from typing import Dict, TYPE_CHECKING

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterSignalIdle, ClusterDistributePrompt
)

from .state_handler import StateHandler
from .state_result import StateResult

from ..instance import ThisInstance
from execution import validate_prompt
from ..env_vars import EnvVars
from ..queued import IncomingBuffer, IncomingMessage
from ..synced_random import SyncedRandom
from ..nodes.nodes import NODE_RECEIVERS
from ..expected_msg import EXPECT_DISTRIBUTE_PROMPT_MSG_KEY

class IdleStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):
        super().__init__(instance, ClusterState.IDLE, ClusterMessageType.SIGNAL_IDLE | ClusterMessageType.DISTRIBUTE_PROMPT)
        self._received_distribute_prompt_msg: bool = False

    def _flush_output_node_cache(self, prompt_json):
        """
        Recursively search through workflow to find nodes of types in NODE_RECEIVERS
        and return their IDs to flush their cached outputs.
        """
        output_node_ids = []
        
        if 'workflow' in prompt_json and isinstance(prompt_json['workflow'], dict):
            workflow = prompt_json['workflow']
            
            for node_id, node_data in workflow.items():
                if 'class_type' in node_data:
                    class_type = node_data['class_type']
                    
                    # Check if this node's class_type is in the NODE_RECEIVERS list
                    for receiver_class in NODE_RECEIVERS:
                        if class_type == receiver_class.__name__:
                            output_node_ids.append(node_id)
                            break
        
        return output_node_ids

    async def handle_state(self, current_state: int) -> StateResult | None:
        if self._received_distribute_prompt_msg:
            return

        result = await self._instance.cluster.udp_message_handler.await_expected_message_thread_safe(EXPECT_DISTRIBUTE_PROMPT_MSG_KEY)
        if not result.success or not result.data:
            raise Exception("Failed to receive fanout tensor metadata")

        incoming_message: IncomingMessage = result.data
        distribute_prompt = ParseDict(incoming_message.message, ClusterDistributePrompt())

        prompt_json = json.loads(distribute_prompt.prompt)
        json_data = {
            'prompt': prompt_json['output'],
            'extra_data': { 'extra_pnginfo': prompt_json['workflow'] },
            "client_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
            # "output_node_ids": self._flush_output_node_cache(prompt_json)
        }

        url = f"http://localhost:{EnvVars.get_comfy_port()}/prompt"
        try:
            response = requests.post(url, json=json_data)
            response.raise_for_status()
            logger.info("Successfully posted prompt to local ComfyUI instance")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error posting prompt: {str(e)}")
        
        self._received_distribute_prompt_msg = True

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        return

    async def handle_buffer(self, current_state: int, incoming_buffer: IncomingBuffer):
        return