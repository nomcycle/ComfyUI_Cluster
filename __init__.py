import requests
import io
import torch
import asyncio
import traceback

from aiohttp import web
from server import PromptServer

from .instance_loop import InstanceLoop, get_instance_loop, instance_loop
from .protobuf.messages_pb2 import ClusterRole
from .env_vars import EnvVars
from .log import logger

class SyncedNode:

    instance: InstanceLoop = get_instance_loop()
    node_count = 0

    def __init__(self):
        SyncedNode.node_count += 1
        self._node_instance_id = SyncedNode.node_count

class ClusterInstanceIndex(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {}

    RETURN_TYPES = ("INT",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self):
        return (EnvVars.get_instance_index(),)

class ClusterFanInTensorsToBatch(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "input": ("IMAGE",),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def blocking_sync(self, input):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(instance._this_instance.fanin_tensor(input))

    def execute(self, input):
        try:
            output = self.blocking_sync(input[0])
            return (output,)
        except Exception as e:
            logger.error("Error executing fan in tensors: %s\n%s", str(e), traceback.format_exc())
            raise e

@PromptServer.instance.routes.post("/cluster/queue")
async def queue(request):
    try:
        prompt_data = await request.json()
        instance: InstanceLoop = get_instance_loop()
        await instance._this_instance.distribute_prompt(prompt_data)
        logger.info("Successfully queued prompt for distribution.")
        return web.Response(status=200)
    except Exception as e:
        logger.error("Error handling request", exc_info=True)
        return web.Response(status=500)

NODE_CLASS_MAPPINGS = {
    "ClusterFanInTensorsToBatch": ClusterFanInTensorsToBatch,
    "ClusterInstanceIndex": ClusterInstanceIndex
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ClusterFanInTensorsToBatch": "Cluster Fan in Tensors to Batch",
    "ClusterInstanceIndex": "Cluster Instance Index"
}

WEB_DIRECTORY = "./js"
__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]
