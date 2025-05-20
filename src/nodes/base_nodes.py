from ..instance_loop import InstanceLoop, get_instance_loop
from abc import abstractmethod
from server import PromptServer


class Anything(str):
    def __ne__(self, _):
        return False


class SyncedNode:
    instance: InstanceLoop = get_instance_loop()
    node_count = 0

    def __init__(self):
        SyncedNode.node_count += 1
        self._node_instance_id = SyncedNode.node_count

class ClusterNodePair:
    @abstractmethod
    def get_pair_type(self) -> str:
        """Return the type of node this pairs with"""
        raise NotImplementedError("get_pair_type() must be implemented by subclasses")

    @abstractmethod
    def get_start_type(self) -> str:
        return self.get_pair_type()

    @abstractmethod
    def get_end_type(self) -> str:
        return self.get_pair_type()

def find_subgraph_start_node(subgraph_id):
    """
    Find a ClusterStartSubgraph node with the matching subgraph_id.
    
    Args:
        subgraph_id (str): The subgraph_id to search for
        
    Returns:
        str: The node ID of the matching ClusterStartSubgraph node
        
    Raises:
        ValueError: If no matching node is found
    """
    # Get the current queue to access the prompt
    current_queue = PromptServer.instance.prompt_queue.get_current_queue()
    item, item_id = current_queue
    _, _, prompt, _, _ = item[0]

    # Find the ClusterStartSubgraph node with matching subgraph_id
    for node_id, node_data in prompt.items():
        if (node_data.get("class_type") == "ClusterStartSubgraph" and
            node_data.get("inputs", {}).get("subgraph_id") == subgraph_id):
            return node_id

    # No matching node found
    raise ValueError(f"No ClusterStartSubgraph found with subgraph_id: {subgraph_id}")
