import asyncio
import torch
import json
import os

from ..instance_loop import InstanceLoop, get_instance_loop
from ..log import logger


def prepare_loop() -> tuple[asyncio.AbstractEventLoop, InstanceLoop]:
    """Helper function to create and prepare a new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    instance: InstanceLoop = get_instance_loop()
    return loop, instance


def get_interconnected_nodes(prompt, fan_out_id):
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
        logger.error(
            f"Fan-out node with ID {fan_out_id} not found in prompt"
        )
        return {}

    # Find all fan-in nodes in the workflow
    all_fan_in_nodes = []
    for node_id, node_data in prompt.items():
        if (
            "class_type" in node_data
            and node_data["class_type"] == "ClusterFanInImages"
        ):
            all_fan_in_nodes.append(node_id)

    if not all_fan_in_nodes:
        logger.error("No ClusterFanInImages nodes found in prompt")
        return {}

    # For each fan-in node, check if there's a clean path to our fan-out node
    valid_fan_in_nodes = []

    for fan_in_id in all_fan_in_nodes:
        path_exists = find_clean_path(prompt, fan_in_id, fan_out_id)
        if path_exists:
            valid_fan_in_nodes.append(fan_in_id)

    if not valid_fan_in_nodes:
        logger.error(
            f"No clean path found from any fan-in node to fan-out node {fan_out_id}"
        )
        return {}

    # Build the subgraph between the fan-out node and all valid fan-in nodes
    return build_subgraph(prompt, fan_out_id, valid_fan_in_nodes)


def find_clean_path(prompt, fan_in_id, fan_out_id):
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
        if "inputs" in current_node:
            for input_name, input_value in current_node["inputs"].items():
                # Check if this input is connected to another node
                if isinstance(input_value, list) and len(input_value) >= 2:
                    connected_node_id = input_value[0]

                    # If we found our fan-out node, we have a path
                    if connected_node_id == fan_out_id:
                        return True

                    # If we encountered another fan node, this path is invalid
                    if (
                        connected_node_id in prompt
                        and "class_type" in prompt[connected_node_id]
                    ):
                        node_type = prompt[connected_node_id]["class_type"]
                        if (
                            node_type == "ClusterFanOutImage"
                            or node_type == "ClusterFanInImages"
                        ):
                            continue

                    # Continue traversing backwards if we haven't visited this node
                    if (
                        connected_node_id not in visited
                        and connected_node_id in prompt
                    ):
                        visited.add(connected_node_id)
                        to_visit.append(connected_node_id)

    # If we've exhausted all paths and haven't found the fan-out node, no clean path exists
    return False


def build_subgraph(prompt, fan_out_id, fan_in_nodes):
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
        if "inputs" in current_node:
            for input_name, input_value in current_node["inputs"].items():
                # Check if this input is connected to another node
                if isinstance(input_value, list) and len(input_value) >= 2:
                    connected_node_id = input_value[0]

                    # If this is our fan-out node, we've reached the boundary
                    if connected_node_id == fan_out_id:
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
        # Skip validation for fan-out and fan-in nodes
        if node_id == fan_out_id or node_id in fan_in_nodes:
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