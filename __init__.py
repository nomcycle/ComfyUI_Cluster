from aiohttp import web
from server import PromptServer

from .instance_loop import InstanceLoop, get_instance_loop
from .log import logger
from .nodes import NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS

@PromptServer.instance.routes.post("/cluster/queue")
async def queue(request):
    try:
        prompt_data = await request.json()
        instance: InstanceLoop = get_instance_loop()
        await instance._this_instance.distribute_prompt(prompt_data)
        logger.info("Successfully queued prompt for distribution.")
        return web.Response(status=200)
    except Exception:
        logger.error("Error handling request", exc_info=True)
        return web.Response(status=500)


WEB_DIRECTORY = "./js"
__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]
