import torch
import asyncio
import traceback

from abc import abstractmethod
from server import PromptServer

from .base_nodes import SyncedNode, ClusterNodePair
from .utils import prepare_loop, get_subgraph, replace_nodes, add_preview_nodes
from ..instance_loop import InstanceLoop, get_instance_loop
from ..env_vars import EnvVars
from ..log import logger

prompt_queue = PromptServer.instance.prompt_queue

# Global registry to store paired class relationships by key
_type_registry = {}

def declare_subgraph_start_node(key):
    """
    Decorator for start nodes that establishes type pairing via a shared key.
    The decorated class will have get_start_type() automatically implemented.
    
    Usage: @start_type('my_pair_key')
           class MyStartNode: ...
    """
    def decorator(cls):
        # Register this class as a start type for the given key
        if key not in _type_registry:
            _type_registry[key] = {}
        _type_registry[key]['start'] = cls.__name__

        # Define get_start_type method
        def get_start_type(self):
            return type(self).__name__

        # Apply the method to the class
        cls.get_start_type = get_start_type

        # Define method to get the paired end type
        def get_pair_end_type(self):
            end_type = _type_registry.get(key, {}).get('end')
            if not end_type:
                raise ValueError(f"No end type registered for key '{key}'")
            return end_type

        # Store the method for getting the paired end type
        cls.get_pair_end_type = get_pair_end_type

        return cls
    return decorator

def declare_subgraph_end_node(key):
    """
    Decorator for end nodes that establishes type pairing via a shared key.
    The decorated class will have get_end_type() automatically implemented.
    
    Usage: @end_type('my_pair_key')
           class MyEndNode: ...
    """
    def decorator(cls):
        # Register this class as an end type for the given key
        if key not in _type_registry:
            _type_registry[key] = {}
        _type_registry[key]['end'] = cls.__name__

        # Define get_end_type method
        def get_end_type(self):
            return type(self).__name__

        # Apply the method to the class
        cls.get_end_type = get_end_type

        # Define method to get the paired start type
        def get_pair_start_type(self):
            start_type = _type_registry.get(key, {}).get('start')
            if not start_type:
                raise ValueError(f"No start type registered for key '{key}'")
            return start_type

        # Store the method for getting the paired start type
        cls.get_pair_start_type = get_pair_start_type

        return cls
    return decorator

class ClusterTensorNodeBase(SyncedNode):
    def __init__(self):
        super().__init__()

    INPUT_TYPE = None  # Set by child classes

    @classmethod
    def INPUT_TYPES(s):
        return {
            "hidden": {
                "unique_id": "UNIQUE_ID",
            },
            "required": {
                "input": (s.INPUT_TYPE,),
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

    def blocking_sync(self, unique_id, input):
        loop, instance = self._prepare_loop()
        instance: InstanceLoop = get_instance_loop()
        return loop.run_until_complete(self._sync_operation(instance, unique_id, input))

    @abstractmethod
    async def _sync_operation(self, instance, unique_id, input):
        pass

    def execute(self, unique_id: str, input):
        try:
            output = self.blocking_sync(unique_id, self.get_input(input))
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

@declare_subgraph_end_node('tensor_fan')
class ClusterFanInBase(ClusterTensorNodeBase, ClusterNodePair):
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)

    async def _sync_operation(self, instance, unique_id, input):
        if EnvVars.get_instance_index() == 0:
            return await instance._this_instance.receive_tensor_fanin(input)
        return await instance._this_instance.send_tensor_to_leader(input)


@declare_subgraph_start_node('tensor_fan')
class ClusterFanOutBase(ClusterTensorNodeBase, ClusterNodePair):

    # @classmethod
    # def IS_CHANGED(s, image):
    #     return float('nan')

    async def _sync_operation(self, instance, unique_id, input):
        if EnvVars.get_instance_index() == 0:
            if (
                len(input.shape) != 4
                or input.shape[0] < EnvVars.get_instance_count()
            ):
                raise ValueError(
                    f"Input must be a batch with at least: {EnvVars.get_instance_count()} tensors."
                )

            from .subgraph import SubgraphProcessor

            # Extract the subgraph
            subgraph = get_subgraph(unique_id, self.get_start_type(), self.get_pair_end_type(), exclude_boundaries=False)

            # Process any nested ClusterUseSubgraph nodes
            subgraph = SubgraphProcessor.process_nested_subgraphs(subgraph)

            # Apply modifications as needed
            subgraph = replace_nodes(subgraph, self.get_start_type(), ClusterListenTensorBroadcast.__name__, clear_inputs=True)
            subgraph = add_preview_nodes(subgraph, self.get_pair_end_type())

            await instance._this_instance.distribute_prompt(
                {
                    "output": subgraph,
                    "workflow": {},
                }
            )
            return await instance._this_instance.fanout_tensor(input)
        return await instance._this_instance.receive_tensor_fanout()


class ClusterGatherBase(ClusterTensorNodeBase):
    OUTPUT_IS_LIST = (True,)
    RETURN_TYPES = ("IMAGE",)

    async def _sync_operation(self, instance, unique_id, input):
        return await instance._this_instance.gather_tensors(input)


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
        return loop.run_until_complete(self._sync_operation(instance, unique_id))

    async def _sync_operation(self, instance, unique_id):
        return await instance._this_instance.receive_tensor_fanout()

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

    async def _sync_operation(self, instance, unique_id, input):
        return await instance._this_instance.broadcast_tensor(input)
