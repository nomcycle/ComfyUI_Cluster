from aiohttp import web
from server import PromptServer

async def with_temporary_endpoint(path, handler, method="POST"):
    route = None
    if method == "POST":
        route = PromptServer.instance.routes.post(path)(handler)
    elif method == "GET":
        route = PromptServer.instance.routes.get(path)(handler)
    
    try:
        yield
    finally:
        if route in PromptServer.instance.routes:
            PromptServer.instance.routes.remove(route)