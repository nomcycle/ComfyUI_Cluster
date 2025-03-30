import asyncio

from server import PromptServer
from ..instance_loop import InstanceLoop, get_instance_loop
from ..log import logger


def prepare_loop() -> tuple[asyncio.AbstractEventLoop, InstanceLoop]:
    """Helper function to create and prepare a new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    instance: InstanceLoop = get_instance_loop()
    return loop, instance


def get_subgraph(unique_id, start_node_type: str, end_node_type: str):
    current_queue = (
        PromptServer.instance.prompt_queue.get_current_queue()
    )
    item, item_id = current_queue
    _, _, prompt, _, _ = item[0]

    if unique_id not in prompt:
        raise ValueError(f"No node found with unique_id {unique_id} and type {start_node_type}")

    # Get the interconnected nodes between start_node and end nodes of type end_node_type
    return get_interconnected_nodes(prompt, unique_id, end_node_type)


def replace_nodes_in_subgraph(subgraph, target_type, replacement_type, clear_inputs=False):
    """
    Replace all nodes of target_type with replacement_type in the subgraph.
    
    Args:
        subgraph (dict): The subgraph to modify
        target_type (str): The class_type to replace
        replacement_type (str): The new class_type
        clear_inputs (bool): Whether to clear inputs on replaced nodes
        
    Returns:
        dict: The modified subgraph
    """
    for node_id, node_data in subgraph.items():
        if node_data.get("class_type") == target_type:
            node_data["class_type"] = replacement_type
            if clear_inputs:
                node_data["inputs"] = {}

    return subgraph


def add_preview_nodes_after_type(subgraph, target_type):
    """
    Add PreviewImage nodes after all nodes of target_type in the subgraph.
    
    Args:
        subgraph (dict): The subgraph to modify
        target_type (str): The class_type to add preview nodes after
        
    Returns:
        dict: The modified subgraph
    """
    for node_id, node_data in list(subgraph.items()):
        if node_data.get("class_type") == target_type:
            # Create a new PreviewImage node
            preview_node_id = f"{node_id}_preview"
            subgraph[preview_node_id] = {
                "class_type": "PreviewImage",
                "inputs": {
                    "images": [
                        node_id,
                        0,
                    ]  # Connect to the output of the target node
                },
            }

    return subgraph


def build_executable_subgraph(subgraph, start_node_type, end_node_type):
    """
    Build an executable subgraph for node expansion.
    
    Args:
        subgraph (dict): The subgraph definition
        start_node_type (str): The class type of the start node
        end_node_type (str): The class type of the end node
        
    Returns:
        dict: Dictionary containing graph, node_mapping, and output_node to be used for node expansion
        
    Raises:
        ValueError: If subgraph is invalid or build process fails
    """
    if not subgraph:
        raise ValueError("No valid subgraph provided")

    # Import graph utilities for node expansion
    from comfy_execution.graph_utils import GraphBuilder, is_link

    # Create a graph builder with a unique prefix
    graph = GraphBuilder()

    # Create a mapping from original node IDs to new nodes in our graph
    node_mapping = {}
    output_node = None

    # First pass: create all nodes
    for node_id, node_data in subgraph.items():
        # Create a new node in our graph
        new_node = graph.node(
            node_data["class_type"],
            id=node_id,  # GraphBuilder will prefix this ID
            **{k: v for k, v in node_data.get("inputs", {}).items() if not is_link(v)}
        )

        node_mapping[node_id] = new_node

        # If this is an end node, it will be our output
        if node_data["class_type"] == end_node_type:
            output_node = new_node

    # Second pass: connect all internal nodes (not handling external inputs)
    for node_id, node_data in subgraph.items():
        if "inputs" not in node_data:
            continue

        current_node = node_mapping[node_id]

        for input_name, input_value in node_data["inputs"].items():
            if is_link(input_value):
                source_node_id, output_idx = input_value
                # Only connect if the source node is in our subgraph (internal connection)
                if source_node_id in node_mapping:
                    source_node = node_mapping[source_node_id]
                    current_node.set_input(input_name, source_node.out(output_idx))
                # External connections will be handled by the caller

    # Return necessary components for the caller to finish setting up the subgraph
    return {
        "graph": graph,
        "node_mapping": node_mapping,
        "output_node": output_node
    }

def connect_inputs_and_finalize(subgraph_components, external_inputs):
    """
    Connect external inputs to a subgraph and finalize it for node expansion.
    
    Args:
        subgraph_components (dict): Dictionary with graph, node_mapping, and output_node
        external_inputs (list): List of tuples (node_id, input_name, value) to connect
        
    Returns:
        dict: An expansion dictionary with 'result' and 'expand' keys
        
    Raises:
        ValueError: If subgraph components are invalid or finalization fails
    """
    if not subgraph_components:
        raise ValueError("Invalid subgraph components")

    graph = subgraph_components["graph"]
    node_mapping = subgraph_components["node_mapping"]
    output_node = subgraph_components["output_node"]

    # Connect all external inputs
    for node_id, input_name, value in external_inputs:
        if node_id in node_mapping:
            node = node_mapping[node_id]
            node.set_input(input_name, value)

    # If we found an output node, return the expansion
    if not output_node:
        raise ValueError("No output node found in subgraph")

    return {
        "result": (output_node.out(0),),  # The subgraph's output
        "expand": graph.finalize(),       # The expanded graph
    }

def get_interconnected_nodes(prompt, start_id, end_node_type):
    """
    Extract a subgraph of interconnected nodes between a start node and end nodes of a specified type.

    Args:
        prompt (dict): The ComfyUI workflow prompt
        start_id (str): The ID of the start node
        end_node_type (str, optional): The class type of end nodes to look for

    Returns:
        dict: A subgraph containing only the nodes between start and end, including those nodes
    """
    # Ensure the start_id exists in the prompt
    if start_id not in prompt:
        raise ValueError(f"Start node with ID {start_id} not found in prompt")

    # If end_node_type is not provided, we can't proceed
    if not end_node_type:
        raise ValueError("End node type not provided")

    # Find all potential end nodes in the workflow
    all_end_nodes = []
    for node_id, node_data in prompt.items():
        if (
            "class_type" in node_data
            and node_data["class_type"] == end_node_type
        ):
            all_end_nodes.append(node_id)

    if not all_end_nodes:
        raise ValueError(f"No {end_node_type} nodes found in prompt")

    # For each end node, check if there's a clean path to our start node
    valid_end_nodes = []

    for end_id in all_end_nodes:
        path_exists = find_clean_path(prompt, end_id, start_id, end_node_type)
        if path_exists:
            valid_end_nodes.append(end_id)

    if not valid_end_nodes:
        raise ValueError(
            f"No clean path found from any {end_node_type} node to start node {start_id}"
        )

    # Build the subgraph between the start node and all valid end nodes
    return build_subgraph(prompt, start_id, valid_end_nodes)


def find_clean_path(prompt, end_id, start_id, end_type=None):
    """
    Check if there's a clean path from end_id to start_id without encountering
    any other cluster nodes of the same types.

    Args:
        prompt (dict): The workflow prompt
        end_id (str): The ID of the end node
        start_id (str): The ID of the start node
        end_type (str, optional): The class type of end nodes to avoid in the path

    Returns:
        bool: True if a clean path exists, False otherwise
    """
    # Get the class type of the start node
    start_type = prompt[start_id].get("class_type", "")

    # If start node doesn't have a class type, we can't proceed
    if not start_type:
        raise ValueError("Start node does not have a class type")

    # If end_type is not provided, use the class type of end_id
    if not end_type:
        end_type = prompt[end_id].get("class_type", "")
        if not end_type:
            return False

    visited = set()
    to_visit = [end_id]
    visited.add(end_id)

    while to_visit:
        current_id = to_visit.pop(0)
        current_node = prompt[current_id]

        # Check if this node has inputs
        if "inputs" in current_node:
            for input_name, input_value in current_node["inputs"].items():
                # Check if this input is connected to another node
                if isinstance(input_value, list) and len(input_value) >= 2:
                    connected_node_id = input_value[0]

                    # If we found our start node, we have a path
                    if connected_node_id == start_id:
                        return True

                    # If we encountered another node of the same types, this path is invalid
                    if (
                        connected_node_id in prompt
                        and "class_type" in prompt[connected_node_id]
                    ):
                        node_type = prompt[connected_node_id]["class_type"]
                        # Skip if we find another node of the same types we're looking for
                        if node_type == start_type or node_type == end_type:
                            continue

                    # Continue traversing backwards if we haven't visited this node
                    if (
                        connected_node_id not in visited
                        and connected_node_id in prompt
                    ):
                        visited.add(connected_node_id)
                        to_visit.append(connected_node_id)

    # If we've exhausted all paths and haven't found the start node, no clean path exists
    return False


def build_subgraph(prompt, start_id, end_nodes):
    """
    Build a subgraph that includes all nodes between start_id and end_nodes.

    Args:
        prompt (dict): The workflow prompt
        start_id (str): The ID of the start node
        end_nodes (list): List of valid end node IDs

    Returns:
        dict: The extracted subgraph
    """
    subgraph = {}
    visited = set()
    to_process = end_nodes.copy()

    # Add all end nodes and the start node to our visited set
    # to mark them as boundaries - we don't traverse beyond them
    for node_id in end_nodes:
        visited.add(node_id)
        subgraph[node_id] = prompt[node_id]

    visited.add(start_id)
    subgraph[start_id] = prompt[start_id]

    # Process each node in our queue
    while to_process:
        current_id = to_process.pop(0)
        current_node = prompt[current_id]

        # Explore all inputs of the current node
        if "inputs" in current_node:
            for input_name, input_value in current_node["inputs"].items():
                # Check if this input is connected to another node
                if isinstance(input_value, list) and len(input_value) >= 2:
                    connected_node_id = input_value[0]

                    # If this is our start node, we've reached the boundary
                    if connected_node_id == start_id:
                        continue

                    # If we haven't visited this node yet, add it to our processing queue
                    if (
                        connected_node_id not in visited
                        and connected_node_id in prompt
                    ):
                        visited.add(connected_node_id)
                        to_process.append(connected_node_id)
                        subgraph[connected_node_id] = prompt[
                            connected_node_id
                        ]

    # Final validation pass to ensure nodes are only connected within the subgraph
    for node_id, node_data in list(subgraph.items()):
        # Skip validation for start and end nodes
        if node_id == start_id or node_id in end_nodes:
            continue

        # Check all inputs to ensure they only connect to nodes in our subgraph
        if "inputs" in node_data:
            for input_name, input_value in node_data["inputs"].items():
                if isinstance(input_value, list) and len(input_value) >= 2:
                    connected_node_id = input_value[0]
                    if connected_node_id not in subgraph:
                        # This node is connected to a node outside our subgraph
                        logger.error(
                            f"Node {node_id} connects to node {connected_node_id} outside the subgraph"
                        )
                        # Remove this node from our subgraph
                        del subgraph[node_id]
                        break

    return subgraph


def find_subgraph_start_node(subgraph_id):
    """
    Find a ClusterStartSubgraph node with the matching subgraph_id.
    
    Args:
        subgraph_id (str): The subgraph_id to search for
        
    Returns:
        str: The node ID of the matching ClusterStartSubgraph node, or None if not found
        
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
