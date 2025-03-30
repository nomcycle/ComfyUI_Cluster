from typing import Dict, List, Tuple, Any, Callable, Optional, TypeVar, Iterable, Set, Iterator
from functools import partial

from server import PromptServer
from comfy_execution.graph_utils import is_link

T = TypeVar('T')
NodeId = str
NodeData = Dict[str, Any]
Graph = Dict[NodeId, NodeData]

# ---- Functional graph operations ----

def nodes(graph: Graph) -> Iterator[Tuple[NodeId, NodeData]]:
    """Iterator over all nodes in a graph."""
    return graph.items()

def node_ids(graph: Graph) -> Iterator[NodeId]:
    """Iterator over all node IDs in a graph."""
    return graph.keys()

def where_type(graph: Graph, node_type: str) -> Iterator[Tuple[NodeId, NodeData]]:
    """Filter nodes by type."""
    return ((node_id, node_data) for node_id, node_data in nodes(graph)
            if node_data.get("class_type") == node_type)

def find_single(iterable: Iterable[T], default=None) -> Optional[T]:
    """Find a single item from an iterable or return default."""
    return next(iter(iterable), default)

def inputs(node_data: NodeData) -> Iterator[Tuple[str, Any]]:
    """Iterator over all inputs of a node."""
    return node_data.get("inputs", {}).items()

def inputs_where(node_data: NodeData, predicate: Callable[[str, Any], bool]) -> Iterator[Tuple[str, Any]]:
    """Filter inputs based on a predicate."""
    return ((name, value) for name, value in inputs(node_data) if predicate(name, value))

def link_inputs(node_data: NodeData) -> Iterator[Tuple[str, List]]:
    """Iterator over all link inputs of a node."""
    return ((name, value) for name, value in inputs(node_data) if is_link(value))

def nodes_connected_to(graph: Graph, target_id: NodeId) -> Iterator[Tuple[NodeId, NodeData, str]]:
    """Find all nodes that connect to the target node."""
    for node_id, node_data in nodes(graph):
        for input_name, input_value in link_inputs(node_data):
            if input_value[0] == target_id:
                yield node_id, node_data, input_name

def nodes_receiving_from(graph: Graph, source_id: NodeId) -> Iterator[Tuple[NodeId, NodeData, str, int]]:
    """Find all nodes that receive from the source node."""
    for node_id, node_data in nodes(graph):
        for input_name, input_value in link_inputs(node_data):
            if input_value[0] == source_id:
                yield node_id, node_data, input_name, input_value[1]

def connected_to_any(graph: Graph, node_ids: List[NodeId]) -> Set[NodeId]:
    """Find all nodes connected to any node in the given list."""
    result = set()
    for target_id in node_ids:
        for node_id, _, _ in nodes_connected_to(graph, target_id):
            result.add(node_id)
    return result

def receiving_from_any(graph: Graph, node_ids: List[NodeId]) -> Set[NodeId]:
    """Find all nodes that receive from any node in the given list."""
    result = set()
    for source_id in node_ids:
        for node_id, _, _, _ in nodes_receiving_from(graph, source_id):
            result.add(node_id)
    return result

def exclude(graph: Graph, ids_to_exclude: Set[NodeId]) -> Graph:
    """Create a new graph excluding specific node IDs."""
    return {node_id: node_data for node_id, node_data in nodes(graph)
            if node_id not in ids_to_exclude}

def deep_copy_graph(graph: Graph) -> Graph:
    """Create a deep copy of a graph."""
    return {node_id: node_data.copy() for node_id, node_data in nodes(graph)}

# ---- Prompt access ----

def get_current_prompt() -> Graph:
    """Get the current workflow prompt being executed."""
    current_queue = PromptServer.instance.prompt_queue.get_current_queue()
    item, _ = current_queue
    _, _, prompt, _, _ = item[0]
    return prompt

# ---- Node type detection helpers ----

def is_node_type(node_data: NodeData, node_type: str) -> bool:
    """Check if a node is of the specified type."""
    return node_data.get("class_type") == node_type
