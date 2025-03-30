from typing import Dict, List, Tuple, Any, Callable, Optional, TypeVar, Iterable, Set, Iterator
from functools import partial
import asyncio

from server import PromptServer
from ..log import logger
from comfy_execution.graph_utils import GraphBuilder, is_link
from ..instance_loop import InstanceLoop, get_instance_loop
from .base_nodes import find_subgraph_start_node

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

def find_subgraph_start_node(subgraph_id: str) -> NodeId:
    """Find a ClusterStartSubgraph node with the matching subgraph_id."""
    prompt = get_current_prompt()

    predicate = lambda node_data: (
        node_data.get("class_type") == "ClusterStartSubgraph" and
        node_data.get("inputs", {}).get("subgraph_id") == subgraph_id
    )

    matching_nodes = [(node_id, node_data) for node_id, node_data in nodes(prompt) if predicate(node_data)]

    if not matching_nodes:
        raise ValueError(f"No ClusterStartSubgraph found with subgraph_id: {subgraph_id}")

    return matching_nodes[0][0]

def expand_use_subgraph_nodes(subgraph: Graph) -> Graph:
    """Expand ClusterUseSubgraph nodes in a subgraph."""
    from .workflow_nodes import ClusterUseSubgraph

    # Find nodes to expand
    use_nodes = list(where_type(subgraph, ClusterUseSubgraph.__name__))
    if not use_nodes:
        return subgraph

    # Deep copy the subgraph
    result = deep_copy_graph(subgraph)

    # Expand each node
    for node_id, node_data in use_nodes:
        # Get expansion details
        nested_info = get_use_subgraph_expansion(node_id, node_data)
        nested_subgraph, start_type, end_type = nested_info

        # Get connections
        input_connections = node_data.get("inputs", {})
        output_connections = list(nodes_receiving_from(result, node_id))
        prefix = f"{node_id}_"

        # Build and integrate expanded nodes
        expanded_nodes, start_nodes, end_nodes = build_expanded_subgraph(
            nested_subgraph, prefix, start_type, end_type,
            input_connections, output_connections
        )

        # Connect external inputs to start nodes
        for start_id in start_nodes:
            if "image" in input_connections and is_link(input_connections["image"]):
                expanded_nodes[start_id]["inputs"]["image"] = input_connections["image"]

        # Connect end nodes to outputs
        if end_nodes:
            end_id = end_nodes[0]  # Use the first end node for simplicity
            for target_id, _, input_name, _ in output_connections:
                subgraph[target_id]["inputs"][input_name] = [end_id, 0]

        # Remove original node and add expanded nodes
        del result[node_id]
        result.update(expanded_nodes)

    return result

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

# Various other methods would be updated to use these functional utilities

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

def find_subgraph_end_node(start_node_id: str, end_node_type: str) -> str:
    """
    Find the end node associated with a specific start node by traversing connections.
    
    Args:
        start_node_id: The ID of the start node
        end_node_type: The class type of the end node to find
        
    Returns:
        The ID of the matching end node
    """
    prompt = get_current_prompt()
    end_node_id = None
    end_nodes = []

    # Find the end node by traversing connections from the start node
    for node_id, node_data in nodes(prompt):
        if node_data.get("class_type") == end_node_type:
            # Check if this end node is connected to our subgraph
            visited = set()
            to_visit = [node_id]

            while to_visit and not end_node_id:
                current = to_visit.pop()
                if current in visited:
                    continue
                visited.add(current)

                for inp_name, inp_value in link_inputs(prompt.get(current, {})):
                    connected_id = inp_value[0]
                    if connected_id == start_node_id:
                        end_node_id = node_id
                        end_nodes.append(node_id)
                        break
                    if connected_id not in visited:
                        to_visit.append(connected_id)

            if end_node_id:
                break

    if not end_node_id:
        raise ValueError(f"No matching end node found for start node: {start_node_id}")

    return end_node_id

def build_use_subgraph(unique_id: str, image: tuple, subgraph_id: str, start_node_type: str, end_node_type: str) -> dict:
    """
    Build and expand a subgraph for the ClusterUseSubgraph node.
    
    This function implements the core functionality of ClusterUseSubgraph.execute:
    1. Find the appropriate start/end nodes for the referenced subgraph
    2. Extract the subgraph between these boundary nodes
    3. Create a graph expansion that can replace the ClusterUseSubgraph node
    
    Args:
        unique_id: The unique ID of the ClusterUseSubgraph node
        image: The input image tensor
        subgraph_id: The ID of the subgraph to use
        start_node_type: The class type of start nodes (e.g., "ClusterStartSubgraph")
        end_node_type: The class type of end nodes (e.g., "ClusterEndSubgraph")
    
    Returns:
        A dictionary containing:
          - result: The output of the expanded subgraph
          - expand: The graph expansion specification
    """
    from comfy_execution.graph_utils import GraphBuilder
    # Use utility functions from the current module to avoid circular imports

    # Find the start node with matching subgraph_id
    start_node_id = find_subgraph_start_node(subgraph_id)

    # Find the matching end node
    end_node_id = find_subgraph_end_node(start_node_id, end_node_type)

    # Get the subgraph with boundaries included for accurate connection mapping
    full_subgraph = get_subgraph(start_node_id, start_node_type, end_node_type, exclude_boundaries=False)

    # Find nodes that connect to the end node - these are our output nodes
    output_node_ids = []
    for input_name, input_value in link_inputs(full_subgraph.get(end_node_id, {})):
        output_node_ids.append(input_value[0])

    if not output_node_ids:
        raise ValueError(f"No nodes found that connect to the end node for subgraph_id: {subgraph_id}")

    # Use the first output node
    output_node_id = output_node_ids[0]

    # Create a GraphBuilder instance for node expansion
    graph = GraphBuilder(prefix=f"{unique_id}_")
    node_mapping = {}

    # First pass: create all nodes
    for node_id, node_data in nodes(full_subgraph):
        # Skip boundary nodes - they shouldn't be included in expansion
        if node_data.get("class_type") in [start_node_type, end_node_type]:
            continue

        # Extract non-link inputs
        static_inputs = {k: v for k, v in inputs(node_data) if not is_link(v)}
        # Create the node
        node = graph.node(node_data["class_type"], id=node_id, **static_inputs)
        node_mapping[node_id] = node

    # Second pass: connect internal links
    for node_id, node_data in nodes(full_subgraph):
        # Skip boundary nodes
        if node_data.get("class_type") in [start_node_type, end_node_type]:
            continue

        if node_id in node_mapping:
            current_node = node_mapping[node_id]
            for input_name, input_value in link_inputs(node_data):
                source_id = input_value[0]

                # If source is start node, connect the external input
                if full_subgraph.get(source_id, {}).get("class_type") == start_node_type:
                    current_node.set_input(input_name, image)
                # Otherwise, connect to internal nodes if they exist
                elif source_id in node_mapping:
                    source_node = node_mapping[source_id]
                    current_node.set_input(input_name, source_node.out(input_value[1]))

    # Get the output node that should be used in the result
    if output_node_id not in node_mapping:
        raise ValueError(f"Output node {output_node_id} not found in node mapping")

    output_node = node_mapping[output_node_id]

    # Return the dictionary for node expansion
    return {
        "result": (output_node.out(0),),
        "expand": graph.finalize()
    }
