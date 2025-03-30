import torch
import asyncio
import json
import os

from server import PromptServer

from .base_nodes import SyncedNode, ClusterNodePair, find_subgraph_start_node
from .tensor_nodes import declare_subgraph_start_node, declare_subgraph_end_node
from ..instance_loop import InstanceLoop, get_instance_loop
from .utils import get_subgraph, build_executable_subgraph, connect_inputs_and_finalize

prompt_queue = PromptServer.instance.prompt_queue


class ClusterExecuteWorkflow(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "workflow_path": (
                    "STRING",
                    {
                        "multiline": False,
                        "default": "workflow.json",
                        "list": [
                            f
                            for f in os.listdir("user/default/workflows")
                            if f.endswith(".json")
                        ],
                    },
                ),
                "image": ("IMAGE",),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def _distribute_prompt_from_path_blocking(self, workflow_path: str):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        with open(f"user/default/workflows/{workflow_path}", "r") as f:
            workflow_json = json.load(f)

        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(
            instance._this_instance.distribute_prompt(
                {
                    "output": workflow_json,
                    "workflow": {},
                }
            )
        )

    def execute(self, workflow_path: str, image: tuple) -> torch.tensor:
        self._distribute_prompt_from_path_blocking(workflow_path)
        return (image,)


class ClusterExecuteCurrentWorkflow(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {"required": {"image": ("IMAGE",)}}

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def _distribute_current_prompt_blocking(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        currently_running = prompt_queue.currently_running
        (_, _, prompt, extra_data, _) = next(iter(currently_running.values()))

        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(
            instance._this_instance.distribute_prompt(
                {
                    "output": prompt,
                    "workflow": extra_data["extra_pnginfo"],
                }
            )
        )

    def execute(self, image: tuple) -> torch.tensor:
        self._distribute_current_prompt_blocking()
        return (image,)

class ClusterSubgraph(SyncedNode):
    pass

@declare_subgraph_start_node('subgraph')
class ClusterStartSubgraph(ClusterSubgraph, ClusterNodePair):

    def get_start_type(self) -> str: return type(self).__name__

    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "hidden": {
                "unique_id": "UNIQUE_ID",
            },
            "required":
            {
                "image": ("IMAGE",),
                "subgraph_id": ("STRING", {"default": "subgraph_id"})
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self, unique_id: str, image: tuple, subgraph_id: str) -> torch.tensor:
        return (image,)

@declare_subgraph_end_node('subgraph')
class ClusterEndSubgraph(ClusterSubgraph, ClusterNodePair):

    def get_end_type(self) -> str: return type(self).__name__

    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "hidden": {
                "unique_id": "UNIQUE_ID",
            },
            "required": {
                "image": ("IMAGE",)
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self, unique_id: str, image: tuple) -> torch.tensor:
        return (image,)


class ClusterUseSubgraph(ClusterSubgraph):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "hidden": {
                "unique_id": "UNIQUE_ID",
            },
            "required":
            {
                "image": ("IMAGE",),
                "subgraph_id": ("STRING", {"default": "subgraph_id"})
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self, unique_id: str, image: tuple, subgraph_id: str) -> torch.tensor:
        try:
            # Find the ClusterStartSubgraph node with matching subgraph_id
            start_node_id = find_subgraph_start_node(subgraph_id)

            # Get the subgraph using the found node's ID
            subgraph = get_subgraph(start_node_id,
                                   ClusterStartSubgraph.__name__,
                                   ClusterEndSubgraph.__name__)

            if not subgraph:
                raise ValueError(f"No valid subgraph found between start and end nodes for subgraph_id: {subgraph_id}")

            # First build the basic subgraph structure
            subgraph_components = build_executable_subgraph(
                subgraph,
                ClusterStartSubgraph.__name__,
                ClusterEndSubgraph.__name__
            )

            # Find start nodes to connect inputs to
            external_inputs = []
            for node_id, node_data in subgraph.items():
                if node_data.get("class_type") == ClusterStartSubgraph.__name__:
                    external_inputs.append((node_id, "image", image))

            # Connect inputs and finalize the subgraph
            return connect_inputs_and_finalize(subgraph_components, external_inputs)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error in ClusterUseSubgraph.execute: {str(e)}")
            return (image,)
