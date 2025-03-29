import comfy.model_management as model_management

from server import PromptServer

from .base_nodes import SyncedNode, Anything
from ..log import logger

prompt_queue = PromptServer.instance.prompt_queue


class ClusterFinallyFree(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "anything": (Anything(),),
                "unload_models": ("BOOLEAN", {"default": True}),
                "free_memory": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ()
    OUTPUT_NODE = True
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self, anything, unload_models, free_memory):
        try:
            if hasattr(prompt_queue, "set_flag"):
                if unload_models:
                    prompt_queue.set_flag("unload_models", unload_models)
                    logger.info(f"Set flag to unload models")

                if free_memory:
                    prompt_queue.set_flag("free_memory", free_memory)
                    logger.info(
                        f"Set flag to free memory and reset execution cache"
                    )

            return ()
        except Exception as e:
            logger.error(f"Error freeing memory: {str(e)}")
            logger.error(traceback.format_exc())
            return ()


class ClusterFreeNow(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "anything": (Anything(),),
                "unload_models": ("BOOLEAN", {"default": True}),
                "free_memory": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = (Anything(),)
    OUTPUT_NODE = True
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self, anything, unload_models, free_memory):
        try:
            if unload_models:
                logger.info(f"Immediately unloading models")
                model_management.unload_all_models()

            if free_memory:
                logger.info(f"Immediately freeing memory cache.")
                model_management.soft_empty_cache()
                torch.cuda.empty_cache()
                gc.collect()
            return (anything,)
        except Exception as e:
            logger.error(f"Error freeing memory: {str(e)}")
            logger.error(traceback.format_exc())
            return (anything,)
