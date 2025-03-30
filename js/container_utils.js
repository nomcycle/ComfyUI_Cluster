// Utility functions for container nodes in ComfyUI
import { 
    GraphTraversal,
    TraversalDirection,
    findNodeOfType,
    findConnectedNodes
} from "./graph_traversal.js";

/**
 * Finds the corresponding end node for a container start node
 * @param {Object} startNode - The container start node
 * @param {String} endNodeType - The type of end node to find
 * @returns {Object|null} - The corresponding end node or null if not found
 */
export function findEndNode(startNode, endNodeType) {
    return findNodeOfType(startNode, endNodeType);
}

/**
 * Finds all nodes that are interconnected between start and end nodes
 * @param {Object} startNode - The container start node
 * @param {Object} endNode - The container end node
 * @returns {Array} - Array of interconnected nodes
 */
export function findInterconnectedNodes(startNode, endNode) {
    return findConnectedNodes(endNode, startNode, TraversalDirection.INPUTS);
}

/**
 * Calculates the bounding box between start and end nodes
 * @param {Object} startNode - The container start node
 * @param {Object} endNode - The container end node
 * @returns {Object} - The bounding box {x, y, width, height}
 */
export function calculateBoundsBetweenNodes(startNode, endNode) {
    const margin = startNode.container?.margin || 20;
    
    // Get all interconnected nodes between start and end nodes
    const interconnectedNodes = findInterconnectedNodes(startNode, endNode);
    
    // Initialize bounds calculator with positions of start and end nodes
    const bounds = {
        minX: Math.min(startNode.pos[0], endNode.pos[0]),
        minY: Math.min(startNode.pos[1], endNode.pos[1]),
        maxX: Math.max(
            startNode.pos[0] + startNode.size[0], 
            endNode.pos[0] + endNode.size[0]
        ),
        maxY: Math.max(
            startNode.pos[1] + startNode.size[1], 
            endNode.pos[1] + endNode.size[1]
        )
    };
    
    // Expand bounds to include all interconnected nodes
    for (const node of interconnectedNodes) {
        if (node === startNode || node === endNode) continue;
        
        if (node.pos && node.size) {
            bounds.minX = Math.min(bounds.minX, node.pos[0]);
            bounds.minY = Math.min(bounds.minY, node.pos[1]);
            bounds.maxX = Math.max(bounds.maxX, node.pos[0] + node.size[0]);
            bounds.maxY = Math.max(bounds.maxY, node.pos[1] + node.size[1]);
        }
    }
    
    // Apply margin to the bounds
    bounds.minX -= margin;
    bounds.minY -= margin;
    bounds.maxX += margin;
    bounds.maxY += margin;
    
    return {
        x: bounds.minX,
        y: bounds.minY,
        width: bounds.maxX - bounds.minX,
        height: bounds.maxY - bounds.minY
    };
}

/**
 * Checks if a node is inside container bounds
 * @param {Object} node - The node to check
 * @param {Array} bounds - The bounding box [x, y, width, height]
 * @returns {Boolean} - True if node is inside container
 */
export function isNodeInsideContainer(node, bounds) {
    if (!node || !node.pos || !node.size) return false;
    
    const nx = node.pos[0];
    const ny = node.pos[1];
    const nw = node.size[0];
    const nh = node.size[1];
    
    // Node is inside if its center is inside the container
    const nodeCenterX = nx + nw / 2;
    const nodeCenterY = ny + nh / 2;
    
    return (
        nodeCenterX >= bounds[0] &&
        nodeCenterY >= bounds[1] &&
        nodeCenterX <= bounds[0] + bounds[2] &&
        nodeCenterY <= bounds[1] + bounds[3]
    );
}