import torch
import asyncio
import json
import os

from server import PromptServer

from .base_nodes import SyncedNode, ClusterNodePair
from .tensor_nodes import declare_subgraph_start_node, declare_subgraph_end_node
from ..instance_loop import InstanceLoop, get_instance_loop

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

    def execute(self, unique_id: str, image: tuple, subgraph_id: str) -> dict:
        from .subgraph import SubgraphExpander

        # Directly use the SubgraphExpander
        return SubgraphExpander.expand_subgraph(
            subgraph_id=subgraph_id,
            unique_id=unique_id,
            input_values={"image": image},
            start_node_type=ClusterStartSubgraph.__name__,
            end_node_type=ClusterEndSubgraph.__name__
        )
