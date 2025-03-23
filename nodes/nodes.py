import torch
import asyncio
import traceback
import json
import os
import gc

from abc import abstractmethod

from aiohttp import web
from server import PromptServer

import comfy.model_management as model_management
prompt_queue = PromptServer.instance.prompt_queue
            

from ..instance_loop import InstanceLoop, get_instance_loop
from ..env_vars import EnvVars
from ..log import logger
from ..frontend_pipe import with_temporary_endpoint

class Anything(str):
    def __ne__(self, _):
        return False

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
                    logger.info(f"Set flag to free memory and reset execution cache")

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
            "hidden": {
                "unique_id": "UNIQUE_ID",
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

    # async def await_prompt_handler(self, request):
    #     prompt_data = await request.json()
    #     # Resolve the future with the received prompt data
    #     self.prompt_future.set_result(prompt_data)
    #     return web.Response(status=200)
    #             
    # async def await_prompt(self):
    #     # Create a future to be resolved when prompt data is received
    #     self.prompt_future = asyncio.Future()
    #     
    #     # Register temporary endpoint
    #     route = PromptServer.instance.routes.post("/cluster/prompt")(self.await_prompt_handler)
    #     
    #     try:
    #         # Wait for the future to be resolved
    #         return await self.prompt_future
    #     finally:
    #         # Clean up the route when done
    #         if route in PromptServer.instance.routes:
    #             PromptServer.instance.routes.remove(route)

    def get_interconnected_nodes(self, prompt, fan_out_id):
        """
        Extract a subgraph of interconnected nodes between a ClusterFanOutImage and a ClusterFanInImages node.
        
        Args:
            prompt (dict): The ComfyUI workflow prompt
            fan_out_id (str): The ID of the fan-out node to start from
            
        Returns:
            dict: A subgraph containing only the nodes between fan-out and fan-in, including those nodes
        """
        # Ensure the fan_out_id exists in the prompt
        if fan_out_id not in prompt:
            logger.error(f"Fan-out node with ID {fan_out_id} not found in prompt")
            return {}
            
        # Find all fan-in nodes in the workflow
        all_fan_in_nodes = []
        for node_id, node_data in prompt.items():
            if 'class_type' in node_data and node_data['class_type'] == 'ClusterFanInImages':
                all_fan_in_nodes.append(node_id)
                
        if not all_fan_in_nodes:
            logger.error("No ClusterFanInImages nodes found in prompt")
            return {}
        
        # For each fan-in node, check if there's a clean path to our fan-out node
        valid_fan_in_nodes = []
        
        for fan_in_id in all_fan_in_nodes:
            path_exists = self._find_clean_path(prompt, fan_in_id, fan_out_id)
            if path_exists:
                valid_fan_in_nodes.append(fan_in_id)
                
        if not valid_fan_in_nodes:
            logger.error(f"No clean path found from any fan-in node to fan-out node {fan_out_id}")
            return {}
        
        # Build the subgraph between the fan-out node and all valid fan-in nodes
        return self._build_subgraph(prompt, fan_out_id, valid_fan_in_nodes)
    
    def _find_clean_path(self, prompt, fan_in_id, fan_out_id):
        """
        Check if there's a clean path from fan_in_id to fan_out_id without encountering
        any other ClusterFanOutImage or ClusterFanInImages nodes.
        
        Args:
            prompt (dict): The workflow prompt
            fan_in_id (str): The ID of the fan-in node
            fan_out_id (str): The ID of the fan-out node
            
        Returns:
            bool: True if a clean path exists, False otherwise
        """
        visited = set()
        to_visit = [fan_in_id]
        visited.add(fan_in_id)
        
        while to_visit:
            current_id = to_visit.pop(0)
            current_node = prompt[current_id]
            
            # Check if this node has inputs
            if 'inputs' in current_node:
                for input_name, input_value in current_node['inputs'].items():
                    # Check if this input is connected to another node
                    if isinstance(input_value, list) and len(input_value) >= 2:
                        connected_node_id = input_value[0]
                        
                        # If we found our fan-out node, we have a path
                        if connected_node_id == fan_out_id:
                            return True
                        
                        # If we encountered another fan node, this path is invalid
                        if connected_node_id in prompt and 'class_type' in prompt[connected_node_id]:
                            node_type = prompt[connected_node_id]['class_type']
                            if node_type == 'ClusterFanOutImage' or node_type == 'ClusterFanInImages':
                                continue
                        
                        # Continue traversing backwards if we haven't visited this node
                        if connected_node_id not in visited and connected_node_id in prompt:
                            visited.add(connected_node_id)
                            to_visit.append(connected_node_id)
        
        # If we've exhausted all paths and haven't found the fan-out node, no clean path exists
        return False
    
    def _build_subgraph(self, prompt, fan_out_id, fan_in_nodes):
        """
        Build a subgraph that includes all nodes between fan_out_id and fan_in_nodes.
        
        Args:
            prompt (dict): The workflow prompt
            fan_out_id (str): The ID of the fan-out node
            fan_in_nodes (list): List of valid fan-in node IDs
            
        Returns:
            dict: The extracted subgraph
        """
        subgraph = {}
        visited = set()
        to_process = fan_in_nodes.copy()
        
        # Add all fan-in nodes and the fan-out node to our visited set
        # to mark them as boundaries - we don't traverse beyond them
        for node_id in fan_in_nodes:
            visited.add(node_id)
            subgraph[node_id] = prompt[node_id]
            
        visited.add(fan_out_id)
        subgraph[fan_out_id] = prompt[fan_out_id]
        
        # Process each node in our queue
        while to_process:
            current_id = to_process.pop(0)
            current_node = prompt[current_id]
            
            # Explore all inputs of the current node
            if 'inputs' in current_node:
                for input_name, input_value in current_node['inputs'].items():
                    # Check if this input is connected to another node
                    if isinstance(input_value, list) and len(input_value) >= 2:
                        connected_node_id = input_value[0]
                        
                        # If this is our fan-out node, we've reached the boundary
                        if connected_node_id == fan_out_id:
                            continue
                        
                        # If we haven't visited this node yet, add it to our processing queue
                        if connected_node_id not in visited and connected_node_id in prompt:
                            visited.add(connected_node_id)
                            to_process.append(connected_node_id)
                            subgraph[connected_node_id] = prompt[connected_node_id]
        
        # Final validation pass to ensure nodes are only connected within the subgraph
        for node_id, node_data in list(subgraph.items()):
            # Skip validation for fan-out and fan-in nodes
            if node_id == fan_out_id or node_id in fan_in_nodes:
                continue
                
            # Check all inputs to ensure they only connect to nodes in our subgraph
            if 'inputs' in node_data:
                for input_name, input_value in node_data['inputs'].items():
                    if isinstance(input_value, list) and len(input_value) >= 2:
                        connected_node_id = input_value[0]
                        if connected_node_id not in subgraph:
                            # This node is connected to a node outside our subgraph
                            logger.error(f"Node {node_id} connects to node {connected_node_id} outside the subgraph")
                            # Remove this node from our subgraph
                            del subgraph[node_id]
                            break
        
        return subgraph

    def blocking_sync(self, input, unique_id):
        loop, instance = self._prepare_loop()
        instance: InstanceLoop = get_instance_loop()
        if EnvVars.get_instance_index() == 0:
            current_queue = PromptServer.instance.prompt_queue.get_current_queue()
            item, item_id = current_queue
            _, _, prompt, _, _ = item[0]
            # Replace ClusterFanOutImage with ClusterListenTensorBroadcast in the subgraph
            subgraph = self.get_interconnected_nodes(prompt, unique_id)
            
            # Find all instances of ClusterFanOutImage and replace with ClusterListenTensorBroadcast
            for node_id, node_data in subgraph.items():
                if node_data.get('class_type') == 'ClusterFanOutImage':
                    node_data['class_type'] = 'ClusterListenTensorBroadcast'
                    # Clear inputs since ClusterListenTensorBroadcast has no inputs
                    node_data['inputs'] = {}
            
            # Find all instances of ClusterFanInImages and add a PreviewImage node after it
            for node_id, node_data in list(subgraph.items()):
                if node_data.get('class_type') == 'ClusterFanInImages':
                    # Create a new PreviewImage node
                    preview_node_id = f"{node_id}_preview"
                    subgraph[preview_node_id] = {
                        'class_type': 'PreviewImage',
                        'inputs': {
                            'images': [node_id, 0]  # Connect to the output of ClusterFanInImages
                        }
                    }
            # print(self.get_interconnected_nodes(prompt, unique_id))
            loop.run_until_complete(instance._this_instance.distribute_prompt({
                'output': subgraph,
                'workflow': {},
            }))
        return loop.run_until_complete(self._sync_operation(instance, input))

    @abstractmethod
    def _sync_operation(self, instance, input):
        pass
    
    def execute(self, input, unique_id: str):
        try:
            output = self.blocking_sync(self.get_input(input), unique_id)
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
        return {
            'hidden': {
                "unique_id": "UNIQUE_ID"
            }
        }

    def blocking_sync(self, unique_id):
        loop, instance = self._prepare_loop()
        return loop.run_until_complete(self._sync_operation(instance))

    def _sync_operation(self, instance):
        return instance._this_instance.receive_tensor_fanout()

    def execute(self, unique_id):
        try:
            output = self.blocking_sync(unique_id)
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
        input_types = LoadImage.INPUT_TYPES()
        input_types['hidden']["unique_id"] = "UNIQUE_ID"
        return input_types

    RETURN_TYPES = ("IMAGE",)

    def get_input(self, image) -> torch.Tensor:
        if EnvVars.get_instance_index() == 0:
            image, _ = LoadImage().load_image(image)
            return image
        return None

    def execute(self, image, unique_id):
        try:
            output = self.blocking_sync(self.get_input(image), unique_id)
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

class ClusterFanInBase(ClusterTensorNodeBase):
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)

    def _sync_operation(self, instance, input):
        if EnvVars.get_instance_index() == 0:
            return instance._this_instance.receive_tensor_fanin(input)
        return instance._this_instance.send_tensor_to_leader(input)

class ClusterGatherBase(ClusterTensorNodeBase):
    OUTPUT_IS_LIST = (True,)
    RETURN_TYPES = ("IMAGE",)
    
    def _sync_operation(self, instance, input):
        return instance._this_instance.gather_tensors(input)

class ClusterFanOutBase(ClusterTensorNodeBase):
    def _sync_operation(self, instance, input):
        if EnvVars.get_instance_index() == 0:
            if len(input.shape) != 4 or input.shape[0] < EnvVars.get_instance_count():
                raise ValueError(f"Input must be a batch with at least: {EnvVars.get_instance_count()} tensors.")
            # instance_id is defined but not used - removing
            return instance._this_instance.fanout_tensor(input)
        return instance._this_instance.receive_tensor_fanout()

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

class ClusterFanInImages(ClusterImageNodeMixin, ClusterFanInBase):
    pass

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
            "images_per_list_element": ("INT", {"default": 2, "min": 1, "max": 64}),
        }}

    RETURN_TYPES = ("IMAGE",)
    OUTPUT_IS_LIST = (True,)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, image, images_per_list_element):
        batch_size = image.shape[0]
        
        # Handle edge case: if batch size is smaller than requested images per element
        if batch_size <= images_per_list_element:
            return ([image],)
        
        # Calculate how many list elements we need
        num_elements = (batch_size + images_per_list_element - 1) // images_per_list_element
        
        result = []
        for i in range(num_elements):
            start_idx = i * images_per_list_element
            end_idx = min(start_idx + images_per_list_element, batch_size)
            result.append(image[start_idx:end_idx])
        
        return (result,)

class ClusterInsertAtIndex(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {"required": {
            "batch": ("IMAGE",),
            "images_to_insert": ("IMAGE",),
            "insert_at_index": ("INT", {"default": 0, "min": 0, "max": 10000}),
        }}

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, batch, images_to_insert, insert_at_index):
        # Ensure batch is treated as a batch even if it's a single image
        if len(batch.shape) == 3:  # Single image with shape [H, W, C]
            batch = batch.unsqueeze(0)  # Convert to [1, H, W, C]
        
        batch_size = batch.shape[0]
        
        # Ensure insert_at_index is within valid range
        insert_at_index = min(insert_at_index, batch_size)
        
        # Split the batch at the insertion point
        first_part = batch[:insert_at_index]
        second_part = batch[insert_at_index:]
        
        # Concatenate the parts with the images to insert in between
        result = torch.cat([first_part, images_to_insert, second_part], dim=0)
        
        return (result,)

class ClusterStridedReorder(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {"required": {
            "image": ("IMAGE",),
            "stride": ("INT", {"default": 2, "min": 1, "max": 128}),
        }}

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, image, stride):
        batch_size = image.shape[0]
        
        # Handle edge cases
        if batch_size <= 1 or stride <= 1:
            return (image,)
            
        # Original indices
        indices = list(range(batch_size))
        new_order = []
        
        # Apply strided reordering pattern:
        # For each position within the stride, collect all elements at that position
        for pos in range(stride):
            # Get indices at position pos, pos+stride, pos+2*stride, etc.
            group_indices = indices[pos::stride]
            new_order.extend(group_indices)
            
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