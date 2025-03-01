import requests
import io
import torch
import asyncio
import traceback
import os
import json
from abc import abstractmethod

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


from server import PromptServer
prompt_queue = PromptServer.instance.prompt_queue

class ClusterExecuteWorkflow(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "workflow_path": ("STRING", {
                    "multiline": False,
                    "default": "workflow.json"
                }),
                "image": ("IMAGE",)
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def _distribute_prompt_from_path_blocking(self, workflow_path: str):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        with open(f'user/default/workflows/{workflow_path}', 'r') as f:
            workflow_json = json.load(f)

        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(instance._this_instance.distribute_prompt({
            'output': workflow_json,
            'workflow': {},
        }))

    def execute(self, workflow_path: str, image: tuple) -> torch.tensor:
        self._distribute_prompt_from_path_blocking(workflow_path)
        return (image,)

class ClusterExecuteCurrentWorkflow(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                # "workflow_path": ("STRING", {
                #     "multiline": False,
                #     "default": "workflow.json"
                # }),
                "image": ("IMAGE",)
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def _distribute_current_prompt_blocking(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        currently_running = prompt_queue.currently_running
        (_, _, prompt, extra_data, _) = next(iter(currently_running.values()))

        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(instance._this_instance.distribute_prompt({
            'output': prompt,
            'workflow': extra_data['extra_pnginfo'],
        }))

    def execute(self, image: tuple) -> torch.tensor:
        self._distribute_current_prompt_blocking()
        return (image,)

class ClusterListenTensorBroadcast(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
            }
        }

    RETURN_TYPES = ("IMAGE",) # Set by child classes
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def blocking_sync(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(instance._this_instance.receive_tensor_fanout())

    def execute(self):
        try:
            output = self.blocking_sync()
            return (output,)
        except Exception as e:
            logger.error("Error executing fan in tensors: %s\n%s", str(e), traceback.format_exc())
            raise e

class ClusterBroadcastTensor(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "input": ("IMAGE",),
            }
        }

    RETURN_TYPES = ("IMAGE",) # Set by child classes
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def blocking_sync(self, input: torch.Tensor):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        instance: InstanceLoop = get_instance_loop()
        loop.run_until_complete(instance._this_instance.broadcast_tensor(input))

    def execute(self, input: torch.Tensor):
        try:
            self.blocking_sync(input)
            return (input,)
        except Exception as e:
            logger.error("Error executing fan in tensors: %s\n%s", str(e), traceback.format_exc())
            raise e

class ClusterTensorNodeBase(SyncedNode):
    def __init__(self):
        super().__init__()

    INPUT_TYPE = None # Set by child classes

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "input": (s.INPUT_TYPE,),
            },
        }

    RETURN_TYPES = (None,) # Set by child classes
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    @abstractmethod
    def get_input(self, input) -> torch.Tensor:
        pass

    def blocking_sync(self, input):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(self._sync_operation(instance, input))

    @abstractmethod
    def _sync_operation(self, instance, input):
        pass

    def execute(self, input):
        try:
            output = self.blocking_sync(input)
            return self._prepare_output(output)
        except Exception as e:
            logger.error("Error executing tensor operation: %s\n%s", str(e), traceback.format_exc())
            raise e

    def _prepare_output(self, output):
        return (output,)

class ClusterGatherBase(ClusterTensorNodeBase):
    OUTPUT_IS_LIST = (True,)
    def _sync_operation(self, instance, input):
        return instance._this_instance.gather_tensors(input)

class ClusterFanOutBase(ClusterTensorNodeBase):
    def _sync_operation(self, instance, input):
        if len(input.shape) != 4 or input.shape[0] < EnvVars.get_instance_count():
            raise ValueError(f"Input must be a batch with at least: {EnvVars.get_instance_count()} tensors.")
        instance_id = EnvVars.get_instance_index()
        return instance._this_instance.fanout_tensor(input)

    def _prepare_output(self, output):
        return (output,)

class ClusterImageNodeMixin:
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)
    
    def get_input(self, input) -> torch.Tensor:
        return input[0]

class ClusterLatentNodeMixin:
    INPUT_TYPE = "LATENT"  
    RETURN_TYPES = ("LATENT",)
    
    def get_input(self, input) -> torch.Tensor:
        samples = input['samples']
        if len(samples.shape) == 4 and samples.shape[0] == 1:
            samples = samples.squeeze(0)
        return samples

    def _prepare_output(self, output):
        return ({'samples': output},)

class ClusterMaskNodeMixin:
    INPUT_TYPE = "MASK"
    RETURN_TYPES = ("MASK",)
    
    def get_input(self, input) -> torch.Tensor:
        return input[0]

class ClusterGatherImages(ClusterImageNodeMixin, ClusterGatherBase):
    pass

class ClusterGatherLatents(ClusterLatentNodeMixin, ClusterGatherBase):
    pass

class ClusterGatherMasks(ClusterMaskNodeMixin, ClusterGatherBase):
    pass

class ClusterFanOutImage(ClusterImageNodeMixin, ClusterFanOutBase):
    pass

class ClusterFanOutLatent(ClusterLatentNodeMixin, ClusterFanOutBase):
    pass

class ClusterFanOutMask(ClusterMaskNodeMixin, ClusterFanOutBase):
    pass

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
    "ClusterInstanceIndex": ClusterInstanceIndex,
    "ClusterExecuteWorkflow": ClusterExecuteWorkflow,
    "ClusterExecuteCurrentWorkflow": ClusterExecuteCurrentWorkflow,
    "ClusterGatherImages": ClusterGatherImages,
    "ClusterGatherLatents": ClusterGatherLatents,
    "ClusterGatherMasks": ClusterGatherMasks,
    "ClusterFanOutImage": ClusterFanOutImage,
    "ClusterFanOutLatent": ClusterFanOutLatent,
    "ClusterFanOutMask": ClusterFanOutMask,
    "ClusterBroadcastTensor": ClusterBroadcastTensor,
    "ClusterListenTensorBroadcast": ClusterListenTensorBroadcast,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ClusterGatherImages": "Cluster Gather Images",
    "ClusterGatherLatents": "Cluster Gather Latents", 
    "ClusterGatherMasks": "Cluster Gather Masks",
    "ClusterFanOutImage": "Cluster Fan-out Image",
    "ClusterFanOutLatent": "Cluster Fan-out Latent",
    "ClusterFanOutMask": "Cluster Fan-out Mask",
    "ClusterInstanceIndex": "Cluster Instance Index",
    "ClusterExecuteWorkflow": "Cluster Execute Workflow",
    "ClusterExecuteCurrentWorkflow": "Cluster Execute Current Workflow",
    "ClusterBroadcastTensor": "Cluster Broadcast Tensor",
    "ClusterListenTensorBroadcast": "Cluster Listen Tensor Broadcast"
}

WEB_DIRECTORY = "./js"
__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]
