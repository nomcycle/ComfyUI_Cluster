from .nodes import nodes
from aiohttp import web
from server import PromptServer

from .instance_loop import InstanceLoop, get_instance_loop, instance_loop
from .log import logger
from .nodes.simple_node import SimpleVisualNode

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
    "ClusterFanInImages": nodes.ClusterFanInImages,
    "ClusterGatherImages": nodes.ClusterGatherImages,
    "ClusterGatherLatents": nodes.ClusterGatherLatents,
    "ClusterGatherMasks": nodes.ClusterGatherMasks,
    "ClusterFanOutImage": nodes.ClusterFanOutImage,
    "ClusterFanOutLatent": nodes.ClusterFanOutLatent,
    "ClusterFanOutMask": nodes.ClusterFanOutMask,
    "ClusterBroadcastTensor": nodes.ClusterBroadcastTensor,
    "ClusterListenTensorBroadcast": nodes.ClusterListenTensorBroadcast,
    "ClusterFlattenBatchedImageList": nodes.ClusterFlattenBatchedImageList,
    "ClusterSplitBatchToList": nodes.ClusterSplitBatchToList,
    "ClusterStridedReorder": nodes.ClusterStridedReorder,
    "ClusterInsertAtIndex": nodes.ClusterInsertAtIndex,
    "ClusterFinallyFree": nodes.ClusterFinallyFree,
    "ClusterFreeNow": nodes.ClusterFreeNow,
    "SimpleVisualNode": SimpleVisualNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ClusterInfo": "Cluster Info",
    "ClusterGetInstanceWorkItemFromBatch": "Get Instance Work Item From Batch",
    "ClusterFanInImages": "Fan-in Images",
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
    "ClusterListenTensorBroadcast": "Listen Tensor Broadcast",
    "ClusterFlattenBatchedImageList": "Flatten Batched Image List",
    "ClusterSplitBatchToList": "Split Batch To List",
    "ClusterStridedReorder": "Strided Reorder Images",
    "ClusterInsertAtIndex": "Insert At Index",
    "ClusterFinallyFree": "Finally Free",
    "ClusterFreeNow": "Free Now",
    "SimpleVisualNode": "Simple Visual Node"
}

WEB_DIRECTORY = "./js"
__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]
