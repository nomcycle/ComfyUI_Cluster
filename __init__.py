import requests
import io
import torch

from .instance_loop import InstanceLoop, get_instance_loop
from .protobuf.messages_pb2 import ClusterRole
from .env_vars import EnvVars
from .log import logger

class SyncedNode:

    node_count = 0
    instance: InstanceLoop = get_instance_loop()

    def __init__(self):
        SyncedNode.node_count += 1
        self._node_instance_id = SyncedNode.node_count

class FenceClusteredWorkflow(SyncedNode):
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

    def _build_url (self, addr: str, endpoint: str):
        return f"http://{addr}:{EnvVars.get_comfy_port()}/{endpoint}"

    def execute(self, input):
        # if self.instance._this_instance.role == ClusterRole.LEADER:
        #     for instance_id, instance in self.instance._this_instance.cluster.instances.items():
        #         buffer = io.BytesIO()
        #         torch.save(input, buffer)
        #         url = self._build_url(instance.address, "/cluster/image")
        #         print(f"Sending image data to {url}")
        #         requests.post(url, data=buffer.getvalue())
        #     return (input,)
        # else:
        #     # Return the tensor received from the leader
        #     return (SyncedNode.instance._synced_tensor,)
        return (input,)

from aiohttp import web
from server import PromptServer
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

# 
# @PromptServer.instance.routes.post("/cluster/image")
# async def sync_image(request):
#     data = await request.read()
#     print(f"Received image data of size {len(data)} bytes")
#     tensor = torch.load(io.BytesIO(data))
#     SyncedNode.instance._synced_tensor = tensor
#     return web.Response(status=200)

NODE_CLASS_MAPPINGS = {
    "FenceClusteredWorkflow": FenceClusteredWorkflow
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "FenceClusteredWorkflow": "Sync Clustered Workflow"
}

WEB_DIRECTORY = "./js"
__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]
