from .nodes import nodes
from aiohttp import web
from server import PromptServer

from .instance_loop import InstanceLoop, get_instance_loop, instance_loop
from .log import logger

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
    "ClusterInfo": nodes.ClusterInfo,
    "ClusterExecuteWorkflow": nodes.ClusterExecuteWorkflow,
    "ClusterGetInstanceWorkItemFromBatch": nodes.ClusterGetInstanceWorkItemFromBatch,
    "ClusterExecuteCurrentWorkflow": nodes.ClusterExecuteCurrentWorkflow,
    "ClusterBroadcastLoadedImage": nodes.ClusterBroadcastLoadedImage,
    "ClusterGatherImages": nodes.ClusterGatherImages,
    "ClusterGatherLatents": nodes.ClusterGatherLatents,
    "ClusterGatherMasks": nodes.ClusterGatherMasks,
    "ClusterFanOutImage": nodes.ClusterFanOutImage,
    "ClusterFanOutLatent": nodes.ClusterFanOutLatent,
    "ClusterFanOutMask": nodes.ClusterFanOutMask,
    "ClusterBroadcastTensor": nodes.ClusterBroadcastTensor,
    "ClusterListenTensorBroadcast": nodes.ClusterListenTensorBroadcast,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ClusterInfo": "Cluster Info",
    "ClusterGetInstanceWorkItemFromBatch": "Get Instance Work Item From Batch",
    "ClusterGatherImages": "Gather Images",
    "ClusterGatherLatents": "Gather Latents", 
    "ClusterGatherMasks": "Gather Masks",
    "ClusterFanOutImage": "Fan-out Image",
    "ClusterFanOutLatent": "Fan-out Latent",
    "ClusterFanOutMask": "Fan-out Mask",
    "ClusterInstanceIndex": "Instance Index",
    "ClusterExecuteWorkflow": "Execute Workflow",
    "ClusterExecuteCurrentWorkflow": "Execute Current Workflow",
    "ClusterBroadcastTensor": "Broadcast Tensor",
    "ClusterBroadcastLoadedImage": "Broadcast Loaded Image",
    "ClusterListenTensorBroadcast": "Listen Tensor Broadcast"
}

WEB_DIRECTORY = "./js"
__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]
