"""
ComfyUI Cluster - Distributed tensor processing across multiple ComfyUI instances.
"""
from aiohttp import web
from server import PromptServer

from .src.instance_loop import get_instance_loop
from .src.log import logger
from .src.nodes import NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS

@PromptServer.instance.routes.post("/cluster/queue")
async def queue(request):
    """
    Handle HTTP requests to queue a prompt for distribution across the cluster.
    
    Args:
        request: The HTTP request containing the prompt data
        
    Returns:
        HTTP response indicating success or failure
    """
    try:
        prompt_data = await request.json()
        instance_loop = get_instance_loop()
        
        # Distribute the prompt to all instances
        await instance_loop.instance.distribute_prompt(prompt_data)
        logger.info("Successfully queued prompt for distribution")
        
        return web.Response(status=200)
    except Exception:
        logger.error("Error handling prompt distribution request", exc_info=True)
        return web.Response(status=500)


# Directory for web assets
WEB_DIRECTORY = "./js"

# Exports for ComfyUI
__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]