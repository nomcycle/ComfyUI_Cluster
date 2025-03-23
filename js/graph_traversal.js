// Graph traversal utilities for ComfyUI
import { app } from "../../../scripts/app.js";

// Direction enum for graph traversal
export const TraversalDirection = {
    BOTH: 0,
    OUTPUTS: 1,
    INPUTS: 2
};

// LinkType enum for identifying connection endpoints
export const LinkType = {
    TARGET: 'target_id',
    ORIGIN: 'origin_id'
};

// Core graph traversal utility that handles various traversal options
export const GraphTraversal = {
    /**
     * Traverse the graph from a starting node
     * @param {Object} options - Traversal configuration options
     * @param {Object} options.startNode - The node to start traversal from
     * @param {String} [options.targetNodeType] - Optional node type to search for
     * @param {Number} [options.direction=TraversalDirection.BOTH] - Direction to traverse
     * @param {Function} [options.nodeCallback] - Callback for each visited node
     * @param {Function} [options.shouldStop] - Custom predicate to stop traversal
     * @param {Function} [options.collectNode] - Callback for collecting nodes in result
     * @returns {Object|Array} - Found target node or collected nodes based on options
     */
    traverse: function(options) {
        const {
            startNode,
            targetNodeType,
            direction = TraversalDirection.BOTH,
            nodeCallback,
            shouldStop,
            collectNode
        } = options;
        
        // Set to track visited nodes to avoid cycles
        const visited = new Set();
        visited.add(startNode.id);
        
        // Queue for BFS traversal
        const queue = [];
        const result = collectNode ? new Set() : null;
        
        // Add initial connections to queue based on direction
        this._addConnectionsByDirection(startNode, direction, visited, queue);
        
        // Perform BFS traversal
        while (queue.length > 0) {
            const currentNode = queue.shift();
            
            // Check if we should stop traversal
            if (shouldStop && shouldStop(currentNode)) {
                continue;
            }
            
            // Check if this is the target node type we're looking for
            if (targetNodeType && currentNode.type === targetNodeType) {
                return currentNode;
            }
            
            // Collect node if requested
            if (collectNode) {
                collectNode(currentNode, result);
            }
            
            // Call the node callback if provided
            if (nodeCallback) {
                nodeCallback(currentNode);
            }
            
            // Continue traversing through this node's connections
            this._addConnectionsByDirection(currentNode, direction, visited, queue);
        }
        
        // Return collected nodes or null if searching for target
        return collectNode ? Array.from(result) : null;
    },
    
    // Helper to add connections based on traversal direction
    _addConnectionsByDirection: function(node, direction, visited, queue) {
        if (direction === TraversalDirection.BOTH || direction === TraversalDirection.OUTPUTS) {
            this._addOutputConnections(node, visited, queue);
        }
        
        if (direction === TraversalDirection.BOTH || direction === TraversalDirection.INPUTS) {
            this._addInputConnections(node, visited, queue);
        }
    },
    
    // Helper function to add nodes connected via outputs to the queue
    _addOutputConnections: function(node, visited, queue) {
        if (!node.outputs) return;
        
        for (const output of node.outputs) {
            if (!output.links) continue;
            
            for (const linkId of output.links) {
                this._addNodeFromLink(node, linkId, LinkType.TARGET, visited, queue);
            }
        }
    },
    
    // Helper function to add nodes connected via inputs to the queue
    _addInputConnections: function(node, visited, queue) {
        if (!node.inputs) return;
        
        for (const input of node.inputs) {
            if (!input.link) continue;
            
            this._addNodeFromLink(node, input.link, LinkType.ORIGIN, visited, queue);
        }
    },
    
    // Common function to add a node from a link
    _addNodeFromLink: function(node, linkId, idType, visited, queue) {
        const link = node.graph.links[linkId];
        if (!link) return;
        
        const connectedNodeId = link[idType];
        const connectedNode = node.graph.getNodeById(connectedNodeId);
        
        if (connectedNode && !visited.has(connectedNode.id)) {
            visited.add(connectedNode.id);
            queue.push(connectedNode);
        }
    }
};

// Convenience methods for common traversal patterns
export function findNodeOfType(startNode, targetNodeType, callback) {
    return GraphTraversal.traverse({
        startNode,
        targetNodeType,
        nodeCallback: callback
    });
}

export function traverseOutputsOnly(startNode, targetNodeType, callback) {
    return GraphTraversal.traverse({
        startNode,
        targetNodeType,
        direction: TraversalDirection.OUTPUTS,
        nodeCallback: callback
    });
}

export function traverseInputsOnly(startNode, targetNodeType, callback) {
    return GraphTraversal.traverse({
        startNode,
        targetNodeType,
        direction: TraversalDirection.INPUTS,
        nodeCallback: callback
    });
}

export function findConnectedNodes(startNode, endNode, direction = TraversalDirection.INPUTS) {
    return GraphTraversal.traverse({
        startNode,
        direction,
        shouldStop: node => node.id === endNode.id,
        collectNode: (node, result) => {
            result.add(node.id);
        }
    }).map(id => startNode.graph.getNodeById(id));
}