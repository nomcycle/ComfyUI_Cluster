from typing import Dict, List, Tuple, Any, Optional
from functools import partial
import asyncio

from server import PromptServer
from ..log import logger
from comfy_execution.graph_utils import GraphBuilder, is_link
from ..instance_loop import InstanceLoop, get_instance_loop
from .base_nodes import find_subgraph_start_node
from .graph_utils import *

# Import declarations for backward compatibility
# These will be imported later to avoid circular imports
SubgraphExpander = None

# ---- Prompt access ----

def get_current_prompt() -> Graph:
    """Get the current workflow prompt being executed."""
    current_queue = PromptServer.instance.prompt_queue.get_current_queue()
    item, _ = current_queue
    _, _, prompt, _, _ = item[0]
    return prompt

# ---- Subgraph extraction ----

def get_subgraph(unique_id: str, start_node_type: str, end_node_type: str, exclude_boundaries: bool = True) -> Graph:
    """Extract a subgraph between nodes of specified types."""
    prompt = get_current_prompt()

    if unique_id not in prompt:
        raise ValueError(f"No node found with unique_id {unique_id} and type {start_node_type}")

    # Get the interconnected nodes between start_node and end nodes
    valid_end_nodes = find_valid_end_nodes(prompt, unique_id, end_node_type)

    if not valid_end_nodes:
        raise ValueError(f"No clean path found from any {end_node_type} node to start node {unique_id}")

    return build_subgraph(prompt, unique_id, valid_end_nodes, exclude_boundaries)

def find_valid_end_nodes(prompt: Graph, start_id: NodeId, end_node_type: str) -> List[NodeId]:
    """Find all end nodes with clean paths to the start node."""
    if start_id not in prompt:
        raise ValueError(f"Start node with ID {start_id} not found in prompt")

    # Find all potential end nodes
    all_end_nodes = [node_id for node_id, _ in where_type(prompt, end_node_type)]

    if not all_end_nodes:
        raise ValueError(f"No {end_node_type} nodes found in prompt")

    # Filter for end nodes with clean paths to the start node
    start_type = prompt[start_id].get("class_type", "")
    if not start_type:
        raise ValueError("Start node does not have a class type")

    is_valid = partial(has_clean_path, prompt=prompt, start_id=start_id,
                       start_type=start_type, end_type=end_node_type)

    return [end_id for end_id in all_end_nodes if is_valid(end_id)]

def has_clean_path(end_id: NodeId, prompt: Graph, start_id: NodeId,
                  start_type: str, end_type: str) -> bool:
    """Check if there's a clean path from end_id to start_id."""
    visited = {end_id}
    to_visit = [end_id]

    while to_visit:
        current_id = to_visit.pop(0)

        for _, input_value in link_inputs(prompt[current_id]):
            connected_id = input_value[0]

            # If we found the start node, we have a path
            if connected_id == start_id:
                return True

            # Skip nodes of the same types
            if connected_id in prompt and prompt[connected_id].get("class_type") in (start_type, end_type):
                continue

            # Add unvisited nodes to the queue
            if connected_id not in visited and connected_id in prompt:
                visited.add(connected_id)
                to_visit.append(connected_id)

    return False

def build_subgraph(prompt: Graph, start_id: NodeId, end_nodes: List[NodeId],
                  exclude_boundaries: bool = True) -> Graph:
    """Build a subgraph between start and end nodes."""
    boundary_nodes = {start_id} | set(end_nodes)

    # Start with all boundary nodes
    subgraph = {node_id: prompt[node_id].copy() for node_id in boundary_nodes}
    visited = set(boundary_nodes)
    to_process = end_nodes.copy()

    # Traverse backwards to find all connected nodes
    while to_process:
        current_id = to_process.pop(0)

        for _, input_value in link_inputs(prompt[current_id]):
            connected_id = input_value[0]

            # Skip visited nodes and the start node
            if connected_id in visited:
                continue

            # Add the connected node
            visited.add(connected_id)
            to_process.append(connected_id)
            subgraph[connected_id] = prompt[connected_id].copy()

    # Validate all connections are within the subgraph
    for node_id, node_data in nodes(subgraph):
        if node_id == start_id:
            continue
        for _, input_value in link_inputs(node_data):
            connected_id = input_value[0]
            if connected_id not in subgraph:
                raise ValueError(f"Node {node_id} connects to node {connected_id} outside the subgraph")

    # Handle boundary node exclusion
    if exclude_boundaries:
        # Find new boundary nodes
        entry_nodes = set()
        for node_id, node_data in nodes(subgraph):
            if node_id not in boundary_nodes:
                for _, input_value in link_inputs(node_data):
                    if input_value[0] == start_id:
                        entry_nodes.add(node_id)

        exit_nodes = set()
        for end_id in end_nodes:
            for _, input_value in link_inputs(subgraph.get(end_id, {})):
                if input_value[0] not in boundary_nodes:
                    exit_nodes.add(input_value[0])

        # Remove boundary nodes
        subgraph = exclude(subgraph, boundary_nodes)

    return subgraph

# ---- Subgraph modifications ----

def replace_nodes(subgraph: Graph, target_type: str, replacement_type: str,
                 clear_inputs: bool = False) -> Graph:
    """Replace all nodes of target_type with replacement_type."""
    result = deep_copy_graph(subgraph)

    # Check if any nodes match the target type
    target_nodes = list(where_type(result, target_type))
    if not target_nodes:
        logger.warning(f"No nodes of type {target_type} found to replace")
        return result

    # Replace the class type for matched nodes
    for node_id, node_data in target_nodes:
        node_data["class_type"] = replacement_type
        if clear_inputs:
            node_data["inputs"] = {}

    return result

def add_preview_nodes(subgraph: Graph, target_type: str) -> Graph:
    """Add preview nodes after all nodes of a specific type."""
    result = deep_copy_graph(subgraph)

    # Check if any nodes match the target type
    target_nodes = list(where_type(result, target_type))
    if not target_nodes:
        logger.warning(f"No nodes of type {target_type} found to add preview after")
        return result

    # Add preview nodes
    for node_id, _ in target_nodes:
        preview_id = f"{node_id}_preview"
        result[preview_id] = {
            "class_type": "PreviewImage",
            "inputs": {"images": [node_id, 0]}
        }

    return result

# ---- Node expansion utilities ----

def expand_use_subgraph_nodes(subgraph: Graph) -> Graph:
    """Expand ClusterUseSubgraph nodes in a subgraph."""
    # Use lazy import to avoid circular imports
    from .subgraph import SubgraphProcessor
    return SubgraphProcessor.process_nested_subgraphs(subgraph)

def get_use_subgraph_expansion(node_id: NodeId, node_data: NodeData) -> Optional[Tuple[Graph, str, str]]:
    """Get expansion details for a ClusterUseSubgraph node."""
    from .workflow_nodes import ClusterStartSubgraph, ClusterEndSubgraph

    # Get the subgraph_id
    subgraph_id = node_data.get("inputs", {}).get("subgraph_id")
    if not subgraph_id:
        raise ValueError(f"Node {node_id} has no subgraph_id")

    # Find the start node and get its subgraph
    start_node_id = find_subgraph_start_node(subgraph_id)
    nested_subgraph = get_subgraph(
        start_node_id,
        ClusterStartSubgraph.__name__,
        ClusterEndSubgraph.__name__,
        exclude_boundaries=True
    )

    if not nested_subgraph:
        raise ValueError(f"No valid subgraph found for subgraph_id: {subgraph_id}")

    return (
        nested_subgraph,
        ClusterStartSubgraph.__name__,
        ClusterEndSubgraph.__name__
    )

def build_expanded_subgraph(
    nested_subgraph: Graph, prefix: str, start_type: str, end_type: str,
    input_connections: Dict[str, Any], output_connections: List[Tuple[NodeId, NodeData, str, int]]
) -> Graph:
    """Build an expanded subgraph using GraphBuilder."""
    # Create expanded nodes with GraphBuilder
    graph = GraphBuilder(prefix=prefix)
    node_mapping = {}

    # Original prompt for reference
    prompt = get_current_prompt()

    # First pass: create all nodes
    for nested_id, nested_data in nodes(nested_subgraph):
        static_inputs = {k: v for k, v in inputs(nested_data) if not is_link(v)}
        node = graph.node(nested_data["class_type"], id=nested_id, **static_inputs)
        node_mapping[nested_id] = node

    # Second pass: connect internal nodes
    start_nodes = []
    end_nodes = []

    for nested_id, nested_data in nodes(nested_subgraph):
        node = node_mapping[nested_id]
        prefixed_id = f"{prefix}{nested_id}"

        # Connect internal links
        for input_name, input_value in link_inputs(nested_data):
            source_id = input_value[0]
            if source_id in node_mapping:
                node.set_input(input_name, node_mapping[source_id].out(input_value[1]))

        # Find nodes that originally connected to start/end nodes
        from_start = False
        to_end = False

        # Check if this node receives from a start node
        for source_id, link_info in prompt.items():
            if link_info.get("class_type") == start_type:
                for _, input_value in link_inputs(nested_data):
                    if input_value[0] == source_id:
                        from_start = True
                        break

        # Check if this node sends to an end node
        for end_id, end_data in prompt.items():
            if end_data.get("class_type") == end_type:
                for _, input_value in link_inputs(end_data):
                    if input_value[0] == nested_id:
                        to_end = True
                        break

        if from_start:
            start_nodes.append(prefixed_id)
        if to_end:
            end_nodes.append(prefixed_id)

    # Finalize the graph
    expanded_nodes = graph.finalize()

    return expanded_nodes, start_nodes, end_nodes

# ---- Connection utilities ----

def connect_inputs_and_finalize(subgraph_components: Dict[str, Any], external_inputs: List[Tuple[NodeId, str, Any]]) -> Dict[str, Any]:
    """Connect external inputs to a subgraph and prepare it for node expansion.
    
    This function implements the Node Expansion pattern as described in the ComfyUI documentation.
    Node Expansion allows nodes to return a new subgraph of nodes that should take their place
    in the graph, which enables custom nodes to implement features like loops.
    
    Args:
        subgraph_components: The components of the subgraph from build_executable_subgraph
        external_inputs: List of (node_id, input_name, value) tuples to connect
        
    Returns:
        A dictionary containing:
          - result: A tuple of the outputs (can be finalized values or node outputs)
          - expand: The finalized graph to perform expansion on
    """
    graph_builder = subgraph_components.get("graph")
    node_mapping = subgraph_components.get("node_mapping")
    output_node = subgraph_components.get("output_node")

    if not all([graph_builder, node_mapping, output_node]):
        raise ValueError("Invalid subgraph components provided")

    # Connect external inputs
    for node_id, input_name, value in external_inputs:
        if node_id not in node_mapping:
            raise KeyError(f"Node {node_id} not found in the subgraph node mapping")

        node = node_mapping[node_id]
        node.set_input(input_name, value)

    # Return the dictionary containing result and expand keys for node expansion
    # This format follows the ComfyUI Node Expansion pattern
    return {
        "result": (output_node.out(0),),  # Tuple of outputs from the node
        "expand": graph_builder.finalize(),  # The finalized graph for expansion
    }

# ---- Asyncio utilities ----

def prepare_loop() -> tuple[asyncio.AbstractEventLoop, InstanceLoop]:
    """Helper function to create and prepare a new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    instance: InstanceLoop = get_instance_loop()
    return loop, instance

# Backward compatibility functions that delegate to SubgraphExpander
def find_subgraph_end_node(start_node_id: str, end_node_type: str) -> str:
    """Delegate to SubgraphExpander.find_subgraph_end_node."""
    # Import here to avoid circular imports
    from .subgraph import SubgraphExpander
    return SubgraphExpander.find_subgraph_end_node(start_node_id, end_node_type)

def build_use_subgraph(unique_id: str, image: tuple, subgraph_id: str, start_node_type: str, end_node_type: str) -> dict:
    """Delegate to SubgraphExpander.expand_subgraph."""
    # Import here to avoid circular imports
    from .subgraph import SubgraphExpander
    return SubgraphExpander.expand_subgraph(
        subgraph_id=subgraph_id,
        unique_id=unique_id,
        input_values={"image": image},
        start_node_type=start_node_type,
        end_node_type=end_node_type
    )
