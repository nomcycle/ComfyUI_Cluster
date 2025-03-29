import torch
import asyncio
import traceback
import json

from abc import abstractmethod
from server import PromptServer

from .base_nodes import SyncedNode
from .utils import prepare_loop, get_interconnected_nodes
from ..instance_loop import InstanceLoop, get_instance_loop
from ..env_vars import EnvVars
from ..log import logger

prompt_queue = PromptServer.instance.prompt_queue


class ClusterTensorNodeBase(SyncedNode):
    def __init__(self):
        super().__init__()

    INPUT_TYPE = None  # Set by child classes

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

    RETURN_TYPES = (None,)  # Set by child classes
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    @abstractmethod
    def get_input(self, input) -> torch.Tensor:
        return input

    def _prepare_loop(self) -> tuple[asyncio.AbstractEventLoop, InstanceLoop]:
        return prepare_loop()

    def blocking_sync(self, input, unique_id):
        loop, instance = self._prepare_loop()
        instance: InstanceLoop = get_instance_loop()
        if EnvVars.get_instance_index() == 0:
            current_queue = (
                PromptServer.instance.prompt_queue.get_current_queue()
            )
            item, item_id = current_queue
            _, _, prompt, _, _ = item[0]
            # Replace ClusterFanOutImage with ClusterListenTensorBroadcast in the subgraph
            subgraph = get_interconnected_nodes(prompt, unique_id)

            # Find all instances of ClusterFanOutImage and replace with ClusterListenTensorBroadcast
            for node_id, node_data in subgraph.items():
                if node_data.get("class_type") == "ClusterFanOutImage":
                    node_data["class_type"] = "ClusterListenTensorBroadcast"
                    # Clear inputs since ClusterListenTensorBroadcast has no inputs
                    node_data["inputs"] = {}

            # Find all instances of ClusterFanInImages and add a PreviewImage node after it
            for node_id, node_data in list(subgraph.items()):
                if node_data.get("class_type") == "ClusterFanInImages":
                    # Create a new PreviewImage node
                    preview_node_id = f"{node_id}_preview"
                    subgraph[preview_node_id] = {
                        "class_type": "PreviewImage",
                        "inputs": {
                            "images": [
                                node_id,
                                0,
                            ]  # Connect to the output of ClusterFanInImages
                        },
                    }
            loop.run_until_complete(
                instance._this_instance.distribute_prompt(
                    {
                        "output": subgraph,
                        "workflow": {},
                    }
                )
            )
        return loop.run_until_complete(self._sync_operation(instance, input))

    @abstractmethod
    def _sync_operation(self, instance, input):
        pass

    def execute(self, input, unique_id: str):
        try:
            output = self.blocking_sync(self.get_input(input), unique_id)
            return self._prepare_output(output)
        except Exception as e:
            logger.error(
                "Error executing tensor operation: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            raise e

    def _prepare_output(self, output):
        return (output,)


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
            if (
                len(input.shape) != 4
                or input.shape[0] < EnvVars.get_instance_count()
            ):
                raise ValueError(
                    f"Input must be a batch with at least: {EnvVars.get_instance_count()} tensors."
                )
            return instance._this_instance.fanout_tensor(input)
        return instance._this_instance.receive_tensor_fanout()


class ClusterListenTensorBroadcast(ClusterTensorNodeBase):
    RETURN_TYPES = ("IMAGE",)

    @classmethod
    def IS_CHANGED(cls) -> float:
        return float("nan")

    @classmethod
    def INPUT_TYPES(s):
        return {"hidden": {"unique_id": "UNIQUE_ID"}}

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
            logger.error(
                "Error executing tensor operation: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            raise e


class ClusterBroadcastTensor(ClusterTensorNodeBase):
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)

    def get_input(self, input):
        return input[0]

    def _sync_operation(self, instance, input):
        return instance._this_instance.broadcast_tensor(input)