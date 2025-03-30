from typing import Dict, Any

from comfy_execution.graph_utils import GraphBuilder, is_link, add_graph_prefix
from .base_nodes import find_subgraph_start_node
from .graph_utils import *

# Define utility functions directly to avoid circular imports
NodeId = str
NodeData = Dict[str, Any]
Graph = Dict[NodeId, NodeData]

class SubgraphExpander:
    """Handles expansion of nested subgraphs with proper prefixing and output mapping."""

    @staticmethod
    def expand_subgraph(
        subgraph_id: str,
        unique_id: str,
        input_values: dict,
        start_node_type: str,
        end_node_type: str
    ) -> dict:
        """
        Expand a subgraph with given inputs.
        
        Args:
            subgraph_id: The ID of the subgraph to expand
            unique_id: The unique ID of the node requesting expansion
            input_values: Dictionary of input values to connect to the subgraph
            start_node_type: Class type of the start node
            end_node_type: Class type of the end node
            
        Returns:
            Dictionary with 'result' and 'expand' keys for node expansion
        """
        # Find the start and end nodes
        start_node_id = find_subgraph_start_node(subgraph_id)
        end_node_id = SubgraphExpander.find_subgraph_end_node(start_node_id, end_node_type)

        # Extract the subgraph
        from .utils import get_subgraph
        subgraph = get_subgraph(start_node_id, start_node_type, end_node_type, exclude_boundaries=False)

        # Create a GraphBuilder with proper prefix
        graph = GraphBuilder(prefix=f"{unique_id}_")
        node_mapping = SubgraphExpander._create_nodes(graph, subgraph, start_node_type, end_node_type)

        # Connect inputs and internal links
        SubgraphExpander._connect_nodes(graph, node_mapping, subgraph, input_values, start_node_type)

        # Find the output node
        output_node = SubgraphExpander._find_output_node(node_mapping, subgraph, end_node_id)

        # Return the expansion result
        return {
            "result": (output_node.out(0),),
            "expand": graph.finalize()
        }

    @staticmethod
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

    @staticmethod
    def _create_nodes(graph, subgraph, start_node_type, end_node_type):
        """Create all nodes in the subgraph except boundary nodes."""
        node_mapping = {}

        for node_id, node_data in nodes(subgraph):
            # Skip boundary nodes
            if node_data.get("class_type") in [start_node_type, end_node_type]:
                continue

            # Extract non-link inputs
            static_inputs = {k: v for k, v in inputs(node_data) if not is_link(v)}

            # Create the node
            node = graph.node(node_data["class_type"], id=node_id, **static_inputs)
            node_mapping[node_id] = node

        return node_mapping

    @staticmethod
    def _connect_nodes(graph, node_mapping, subgraph, input_values, start_node_type):
        """Connect internal nodes and handle external inputs."""
        for node_id, node_data in nodes(subgraph):
            # Skip boundary nodes and nodes not in our mapping
            if node_data.get("class_type") in [start_node_type] or node_id not in node_mapping:
                continue

            current_node = node_mapping[node_id]
            for input_name, input_value in link_inputs(node_data):
                source_id = input_value[0]

                # If source is start node, connect the external input
                if subgraph.get(source_id, {}).get("class_type") == start_node_type:
                    # Connect appropriate input value
                    for name, value in input_values.items():
                        current_node.set_input(input_name, value)
                # Otherwise, connect to internal nodes if they exist
                elif source_id in node_mapping:
                    source_node = node_mapping[source_id]
                    current_node.set_input(input_name, source_node.out(input_value[1]))

    @staticmethod
    def _find_output_node(node_mapping, subgraph, end_node_id):
        """Find the node that should provide the output for the expanded subgraph."""
        # Find nodes that connect to the end node
        output_node_ids = []
        for input_name, input_value in link_inputs(subgraph.get(end_node_id, {})):
            output_node_ids.append(input_value[0])

        if not output_node_ids:
            raise ValueError("No nodes found that connect to the end node")

        # Use the first output node
        output_node_id = output_node_ids[0]

        if output_node_id not in node_mapping:
            raise ValueError(f"Output node {output_node_id} not found in node mapping")

        return node_mapping[output_node_id]


class SubgraphProcessor:
    """Processes subgraphs with nested ClusterUseSubgraph nodes."""

    @staticmethod
    def process_nested_subgraphs(subgraph: dict) -> dict:
        """
        Process a subgraph, expanding any nested ClusterUseSubgraph nodes.
        
        Args:
            subgraph: The subgraph to process
            
        Returns:
            Processed subgraph with nested nodes expanded
        """
        # Find nodes with class_type "ClusterUseSubgraph" to avoid direct import
        use_nodes = list(where_type(subgraph, "ClusterUseSubgraph"))
        if not use_nodes:
            return subgraph

        # Deep copy the subgraph
        result = deep_copy_graph(subgraph)

        # Process each node
        for node_id, node_data in use_nodes:
            # Expand the node
            expanded_result = SubgraphProcessor._expand_use_node(node_id, node_data, result)

            # Remove original node and add expanded nodes
            del result[node_id]
            result.update(expanded_result)

        return result

    @staticmethod
    def _expand_use_node(node_id: str, node_data: dict, parent_graph: dict) -> dict:
        """Expand a ClusterUseSubgraph node within a parent graph."""
        # Get subgraph_id and inputs
        subgraph_id = node_data.get("inputs", {}).get("subgraph_id")
        inputs = node_data.get("inputs", {})

        # Get prefix to ensure unique node IDs
        prefix = f"{node_id}_"

        # Get the nested subgraph - use string literals for node types to avoid imports
        expanded_subgraph = SubgraphProcessor._get_expanded_subgraph(
            subgraph_id=subgraph_id,
            prefix=prefix,
            start_node_type="ClusterStartSubgraph",
            end_node_type="ClusterEndSubgraph"
        )

        # Connect inputs to the subgraph
        SubgraphProcessor._connect_inputs(expanded_subgraph, inputs)

        # Connect outputs to parent graph
        output_connections = list(nodes_receiving_from(parent_graph, node_id))
        connections = SubgraphProcessor._connect_to_parent(expanded_subgraph, output_connections)
        
        # Apply connections to parent graph
        if connections:
            for target_id, input_name, source_id in connections:
                parent_graph[target_id]["inputs"][input_name] = [source_id, 0]

        return expanded_subgraph

    @staticmethod
    def _get_expanded_subgraph(subgraph_id, prefix, start_node_type, end_node_type):
        """Get an expanded subgraph with proper node IDs."""
        # Find the start node with matching subgraph_id
        start_node_id = find_subgraph_start_node(subgraph_id)

        # Find the end node
        end_node_id = SubgraphExpander.find_subgraph_end_node(start_node_id, end_node_type)

        # Get the nested subgraph
        from .utils import get_subgraph
        nested_subgraph = get_subgraph(
            start_node_id,
            start_node_type,
            end_node_type,
            exclude_boundaries=True
        )

        # Add prefix to all node IDs
        return add_graph_prefix(nested_subgraph, [], prefix)[0]

    @staticmethod
    def _connect_inputs(expanded_subgraph, inputs):
        """Connect inputs to the expanded subgraph."""
        # Find entry points - nodes that would have been connected to the start node
        if "image" not in inputs or not is_link(inputs["image"]):
            return

        # Get the input image link
        image_link = inputs["image"]

        # Connect to all nodes that need image input
        for node_id, node_data in nodes(expanded_subgraph):
            if "image" in node_data.get("inputs", {}):
                node_data["inputs"]["image"] = image_link

    @staticmethod
    def _connect_to_parent(expanded_subgraph, output_connections):
        """Connect expanded subgraph outputs to the parent graph."""
        if not output_connections:
            return

        # Find leaf nodes in the expanded subgraph
        leaf_nodes = []
        connected_to = set()

        for node_id, node_data in nodes(expanded_subgraph):
            for _, input_value in link_inputs(node_data):
                if input_value[0] in expanded_subgraph:
                    connected_to.add(input_value[0])

        for node_id in expanded_subgraph:
            if node_id not in connected_to:
                leaf_nodes.append(node_id)

        if not leaf_nodes:
            return

        # Use first leaf node as output
        output_node_id = leaf_nodes[0]

        # Return parent connections that need to be updated
        return [(target_id, input_name, output_node_id) for target_id, _, input_name, _ in output_connections]
