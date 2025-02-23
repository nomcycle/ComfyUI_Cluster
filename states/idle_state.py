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
from ..queued import IncomingMessage

class IdleStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):
        super().__init__(instance, ClusterState.IDLE, ClusterMessageType.SIGNAL_IDLE | ClusterMessageType.DISTRIBUTE_PROMPT)

    async def handle_state(self, current_state: int) -> StateResult | None:
        # signal_idle = ClusterSignalIdle()
        # signal_idle.header.type = ClusterMessageType.SIGNAL_IDLE
        # signal_idle.header.require_ack = True
        
        # logger.info("Sending idle signal to followers")
        # await self._instance.cluster.udp.send_and_wait(signal_idle)
        logger.info("Idling...")
        await asyncio.sleep(1.0)

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_PROMPT:
            distribute_prompt = ParseDict(incoming_message.message, ClusterDistributePrompt())
            # prompt_json = json.loads(distribute_prompt.prompt)

            prompt_json = json.loads(distribute_prompt.prompt)
            json_data = {
                'prompt': prompt_json['output'],
                'extra_data': { 'extra_pnginfo': prompt_json['workflow'] },
                "client_id": datetime.now().strftime("%Y%m%d_%H%M%S")
            }
            # Post prompt to local ComfyUI instance
            url = f"http://localhost:{EnvVars.get_comfy_port()}/prompt"
            try:
                response = requests.post(url, json=json_data)
                response.raise_for_status()
                logger.info("Successfully posted prompt to local ComfyUI instance")
                return
                # return StateResult(current_state, self, ClusterState.EXECUTING, ExecutingStateHandler(self._instance))

            except requests.exceptions.RequestException as e:
                logger.error(f"Error posting prompt: {str(e)}")
        else:
            signal_idle = ParseDict(incoming_message.message, ClusterSignalIdle())
            sender_id = signal_idle.header.sender_instance_id
            
            if sender_id not in self._instance.cluster.instances:
                logger.error("Invalid idle signal from unknown instance %s", sender_id)
                return
                
            logger.info("Received idle signal from leader %s", sender_id)