import torch
import asyncio
import traceback
import json
import os
from abc import abstractmethod

from server import PromptServer
prompt_queue = PromptServer.instance.prompt_queue

from ..instance_loop import InstanceLoop, get_instance_loop
from ..env_vars import EnvVars
from ..log import logger

class SyncedNode:

    instance: InstanceLoop = get_instance_loop()
    node_count = 0

    def __init__(self):
        SyncedNode.node_count += 1
        self._node_instance_id = SyncedNode.node_count

class ClusterInfo(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {}

    RETURN_TYPES = ("INT", "INT",)
    RETURN_NAMES = ("Instance Index", "Instance Count",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self):
        return (EnvVars.get_instance_index(), EnvVars.get_instance_count(),)

class ClusterGetInstanceWorkItemFromBatch(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {"required": {"images": ("IMAGE",)}}

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self, images):
        instance_index = EnvVars.get_instance_index()
        instance_count = EnvVars.get_instance_count()
        batch_size = images.shape[0]
        
        if instance_count <= 1:
            return (images,)
        
        # Calculate slice for this instance
        base_size = batch_size // instance_count
        remainder = batch_size % instance_count
        start_idx = instance_index * base_size + min(instance_index, remainder)
        end_idx = start_idx + base_size + (1 if instance_index < remainder else 0)
        
        # Handle edge cases
        if start_idx >= batch_size or start_idx >= end_idx:
            return (torch.zeros((1, *images.shape[1:]), dtype=images.dtype, device=images.device),)
        
        logger.info(f"Instance {instance_index}/{instance_count} processing {end_idx-start_idx} images ({start_idx}-{end_idx-1})")
        return (images[start_idx:end_idx].clone(),)

# Add to NODE_RECEIVERS list for output caching
NODE_RECEIVERS = [ClusterGetInstanceWorkItemFromBatch]

class ClusterExecuteWorkflow(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "workflow_path": ("STRING", {
                    "multiline": False,
                    "default": "workflow.json",
                    "list": [f for f in os.listdir("user/default/workflows") if f.endswith(".json")]
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
        return input

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
            output = self.blocking_sync(self.get_input(input))
            return self._prepare_output(output)
        except Exception as e:
            logger.error("Error executing tensor operation: %s\n%s", str(e), traceback.format_exc())
            raise e

    def _prepare_output(self, output):
        return (output,)

class ClusterListenTensorBroadcast(ClusterTensorNodeBase):
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)

    def get_input(self, input):
        return input

    def _sync_operation(self, instance, input):
        return instance._this_instance.receive_tensor_fanout()

class ClusterBroadcastTensor(ClusterTensorNodeBase):
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)

    def get_input(self, input):
        return input[0]

    def _sync_operation(self, instance, input):
        return instance._this_instance.broadcast_tensor(input)

# Import LoadImage from the correct location
from nodes import LoadImage

class ClusterBroadcastLoadedImage(ClusterBroadcastTensor):
    @classmethod
    def INPUT_TYPES(s):
        return LoadImage.INPUT_TYPES()

    RETURN_TYPES = ("IMAGE",)

    def get_input(self, image) -> torch.Tensor:
        if EnvVars.get_instance_index() == 0:
            image, _ = LoadImage().load_image(image)
            return image
        return None

    def execute(self, image):
        try:
            output = self.blocking_sync(self.get_input(image))
            return self._prepare_output(output)
        except Exception as e:
            logger.error("Error executing tensor operation: %s\n%s", str(e), traceback.format_exc())
            raise e

    @classmethod
    def IS_CHANGED(s, image):
        if EnvVars.get_instance_index() == 0:
            return LoadImage.IS_CHANGED(image)
        return True

    @classmethod
    def VALIDATE_INPUTS(s, image):
        if EnvVars.get_instance_index() == 0:
            return LoadImage.VALIDATE_INPUTS(image)
        return True

    def _sync_operation(self, instance, input):
        if EnvVars.get_instance_index() == 0:
            return instance._this_instance.broadcast_tensor(input)
        return instance._this_instance.receive_tensor_fanout()

class ClusterGatherBase(ClusterTensorNodeBase):
    OUTPUT_IS_LIST = (True,)
    RETURN_TYPES = ("IMAGE",)
    
    def _sync_operation(self, instance, input):
        return instance._this_instance.gather_tensors(input)

class ClusterFanOutBase(ClusterTensorNodeBase):
    def _sync_operation(self, instance, input):
        if len(input.shape) != 4 or input.shape[0] < EnvVars.get_instance_count():
            raise ValueError(f"Input must be a batch with at least: {EnvVars.get_instance_count()} tensors.")
        # instance_id is defined but not used - removing
        return instance._this_instance.fanout_tensor(input)

class ClusterImageNodeMixin:
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)
    
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

# List of receiver nodes such that we can invalidate 
# their cached outputs when re-running prompts.
NODE_RECEIVERS = [
    ClusterGatherImages,
    ClusterGatherLatents,
    ClusterGatherMasks,
    ClusterListenTensorBroadcast
]