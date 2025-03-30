// Container rendering functionality for ComfyUI
import { calculateBoundsBetweenNodes, isNodeInsideContainer } from './container_utils.js';

/**
 * Renders a container between start and end nodes
 * @param {Object} startNode - The container start node
 * @param {Object} endNode - The container end node
 * @param {CanvasRenderingContext2D} ctx - The canvas rendering context
 */
export function drawContainer(startNode, endNode, ctx) {
    if (!startNode || !endNode || !ctx) return;
    
    // Calculate bounds between start and end nodes
    const bounds = calculateBoundsBetweenNodes(startNode, endNode);
    
    // Save current context
    ctx.save();
    
    // Convert to screen coordinates
    const x = bounds.x - startNode.pos[0];
    const y = bounds.y - startNode.pos[1];
    
    // Draw container background with higher transparency
    ctx.fillStyle = startNode.bgcolor + "55"; // More transparent alpha
    ctx.beginPath();
    if (ctx.roundRect) {
        ctx.roundRect(x, y, bounds.width, bounds.height, 8);
    } else {
        ctx.rect(x, y, bounds.width, bounds.height);
    }
    ctx.fill();
    
    // Draw dashed border to indicate it's a container
    ctx.strokeStyle = startNode.container.color;
    ctx.lineWidth = 2;
    ctx.setLineDash([5, 3]);
    ctx.beginPath();
    if (ctx.roundRect) {
        ctx.roundRect(x, y, bounds.width, bounds.height, 8);
    } else {
        ctx.rect(x, y, bounds.width, bounds.height);
    }
    ctx.stroke();
    ctx.setLineDash([]);
    
    // Draw container label
    ctx.fillStyle = startNode.container.color;
    ctx.font = "14px Arial";
    ctx.textAlign = "center";
    ctx.fillText(startNode.container.title || "Container", x + bounds.width/2, y + 25);
    
    // Restore context
    ctx.restore();
    
    // Update the container's bounding area
    startNode.container.bounding = [bounds.x, bounds.y, bounds.width, bounds.height];
    
    return bounds;
}

/**
 * Recalculates which nodes are inside a container
 * @param {Object} startNode - The container start node
 * @param {Object} endNode - The container end node
 */
export function recalculateContainerNodes(startNode, endNode) {
    if (!startNode || !startNode.graph || !startNode.container || !endNode) return;
    
    startNode.container.nodes = [];
    
    // Calculate bounds between start and end nodes
    const bounds = calculateBoundsBetweenNodes(startNode, endNode);
    startNode.container.bounding = [bounds.x, bounds.y, bounds.width, bounds.height];
    
    // Check all nodes in the graph
    const allNodes = startNode.graph._nodes;
    if (!allNodes) return;
    
    for (const node of allNodes) {
        if (node === startNode || node === endNode) continue; // Skip self and partner
        
        // Check if node is inside this container
        if (isNodeInsideContainer(node, startNode.container.bounding)) {
            startNode.container.nodes.push(node);
        }
    }
}