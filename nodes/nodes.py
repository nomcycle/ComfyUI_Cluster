import torch
import asyncio
import traceback
import json
import os
from abc import abstractmethod
import comfy.utils

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

    def _prepare_loop(self) -> tuple[asyncio.AbstractEventLoop, InstanceLoop]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        instance: InstanceLoop = get_instance_loop()
        return loop, instance

    def blocking_sync(self, input):
        loop, instance = self._prepare_loop()
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
    RETURN_TYPES = ("IMAGE",)

    @classmethod
    def IS_CHANGED(cls) -> float:
        return float('nan')

    @classmethod
    def INPUT_TYPES(s):
        return {}

    def blocking_sync(self):
        loop, instance = self._prepare_loop()
        return loop.run_until_complete(self._sync_operation(instance))

    def _sync_operation(self, instance):
        return instance._this_instance.receive_tensor_fanout()

    def execute(self):
        try:
            output = self.blocking_sync()
            return self._prepare_output(output)
        except Exception as e:
            logger.error("Error executing tensor operation: %s\n%s", str(e), traceback.format_exc())
            raise e

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

# Batch list flattening node
class ClusterFlattenBatchedImageList(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {"required": {
            "images": ("IMAGE",),
        }}

    INPUT_IS_LIST = True
    RETURN_TYPES = ("IMAGE",)
    OUTPUT_IS_LIST = (True,)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, images):
        # Flatten a list of batches into a list of individual images
        flattened_images = []
        for batch in images:
            for i in range(batch.shape[0]):
                flattened_images.append(batch[i:i+1])
        
        return (flattened_images,)

class ClusterSplitBatchToList(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {"required": {
            "image": ("IMAGE",),
            "batches_per_list_element": ("INT", {"default": 2, "min": 1, "max": 64}),
        }}

    RETURN_TYPES = ("IMAGE",)
    OUTPUT_IS_LIST = (True,)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, image, batches_per_list_element):
        batch_size = image.shape[0]
        
        # Handle edge case: if batch size is smaller than num_batches
        if batch_size < batches_per_list_element:
            # Return individual images as separate batches
            return ([image[i:i+1] for i in range(batch_size)],)
        
        # Calculate base size for each batch
        base_size = batch_size // batches_per_list_element
        remainder = batch_size % batches_per_list_element
        
        result = []
        start_idx = 0
        
        for i in range(batches_per_list_element):
            # Add one extra item to earlier batches if there's a remainder
            curr_size = base_size + (1 if i < remainder else 0)
            if curr_size > 0:  # Only add non-empty batches
                result.append(image[start_idx:start_idx + curr_size])
                start_idx += curr_size
        
        return (result,)

class ClusterStridedReorder(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {"required": {
            "image": ("IMAGE",),
            "stride": ("INT", {"default": 2, "min": 1, "max": 128}),
            "length": ("INT", {"default": 0, "min": 0, "max": 1024}),
        }}

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, image, stride, length):
        batch_size = image.shape[0]
        
        # Handle edge cases
        if batch_size <= 1 or stride <= 1:
            return (image,)
        
        # If length is 0 or exceeds batch size, use the entire batch
        if length <= 0 or length > batch_size:
            length = batch_size
            
        # Original indices
        indices = list(range(batch_size))
        new_order = []
        
        # Calculate how many complete stride groups we have within the specified length
        num_groups = (length + stride - 1) // stride  # Ceiling division
        
        # Apply strided reordering pattern:
        # For each position within the stride, collect all elements at that position
        for pos in range(stride):
            # Get indices at position pos, pos+stride, pos+2*stride, etc. up to length
            group_indices = indices[pos:length:stride]
            new_order.extend(group_indices)
            
        # Add any remaining indices not included in the strided portion
        if length < batch_size:
            new_order.extend(indices[length:])
            
        # Reorder the batch according to the pattern
        reordered_images = torch.stack([image[idx] for idx in new_order], dim=0)
        return (reordered_images,)

# List of receiver nodes such that we can invalidate 
# their cached outputs when re-running prompts.
NODE_RECEIVERS = [
    ClusterGatherImages,
    ClusterGatherLatents,
    ClusterGatherMasks,
    ClusterListenTensorBroadcast
]