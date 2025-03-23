// Container Node implementation for ComfyUI
import { app } from "../../../scripts/app.js";
import { 
    GraphTraversal,
    TraversalDirection,
    findNodeOfType,
    findConnectedNodes
} from "./graph_traversal.js";

// Register node for the SimpleVisualNode Python class
app.registerExtension({
    name: "ComfyUI.Cluster.ClusterFanOutImage",

    async beforeRegisterNodeDef(nodeType, nodeData, app) {
        // Only apply to our specific node
        if (nodeData.name !== "ClusterFanOutImage") {
            return;
        }
        
        // Specific convenience methods for this node type
        function findFanInNode(fanOutNode) {
            return findNodeOfType(fanOutNode, "ClusterFanInImages");
        }
        
        function findInterconnectedNodes(fanOutNode, fanInNode) {
            return findConnectedNodes(fanInNode, fanOutNode, TraversalDirection.INPUTS);
        }

        nodeType.prototype.onExecute = function() {
            prompt = app.graphToPrompt();
            fetch('/cluster/queue', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(prompt)
            }).then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
            }).catch(error => {
                console.error("Error executing cluster node:", error);
            });
        };

        // Override the node constructor to add custom behavior
        const onNodeCreated = nodeType.prototype.onNodeCreated;
        nodeType.prototype.onNodeCreated = function() {
            const result = onNodeCreated ? onNodeCreated.apply(this, arguments) : undefined;
            
            // Create container properties
            this.container = {
                nodes: [],             // Array of nodes inside this container
                margin: 40,            // Margin around contained nodes
                color: "#5c7a44",      // Default container color
                title: "Cluster Workflow",    // Default title
                isContainer: true,     // Flag to identify as container
                bounding: [0, 0, 300, 200],  // Default size [x, y, width, height]
                lastPos: [0, 0]        // Track last position for movement
            };
            
            // Set custom size for this node
            this.size = [300, 200];
            this.bgcolor = "#334433";
            this.color = "#5c7a44";

            // Store original position for move calculations
            this.container.lastPos = [...this.pos];
            
            return result;
        };
        
        // Add custom drawing method
        nodeType.prototype.onDrawBackground = function(ctx, canvas) {
            if (!this.flags || !this.flags.collapsed) {
                // Find the corresponding FanOut node
                const fanOutNode = findFanInNode(this);
                
                // Only draw container if we have a FanOut node
                if (fanOutNode) {
                    // Calculate bounds between FanIn and FanOut nodes
                    const bounds = calculateBoundsBetweenNodes(this, fanOutNode);
                    
                    // Update container color from widget if available
                    if (this.widgets && this.widgets.length > 1) {
                        const colorWidget = this.widgets.find(w => w.name === "color");
                        if (colorWidget) {
                            this.container.color = colorWidget.value;
                            this.color = colorWidget.value;
                        }
                        
                        const titleWidget = this.widgets.find(w => w.name === "title");
                        if (titleWidget) {
                            this.container.title = titleWidget.value;
                            this.title = titleWidget.value;
                        }
                    }
                    
                    // Save current context
                    ctx.save();
                    
                    // Translate to draw in world coordinates
                    const graphCanvas = app.canvas;
                    if (graphCanvas) {
                        // Convert to screen coordinates
                        const x = bounds.x - this.pos[0];
                        const y = bounds.y - this.pos[1];
                        
                        // Draw container background with higher transparency
                        ctx.fillStyle = this.bgcolor + "55"; // More transparent alpha
                        ctx.beginPath();
                        if (ctx.roundRect) {
                            ctx.roundRect(x, y, bounds.width, bounds.height, 8);
                        } else {
                            ctx.rect(x, y, bounds.width, bounds.height);
                        }
                        ctx.fill();
                        
                        // Draw dashed border to indicate it's a container
                        ctx.strokeStyle = this.container.color;
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
                        ctx.fillStyle = this.container.color;
                        ctx.font = "14px Arial";
                        ctx.textAlign = "center";
                        ctx.fillText(this.container.title || "Container", x + bounds.width/2, y + 25);
                    }
                    
                    // Restore context
                    ctx.restore();
                    
                    // Update the container's bounding area
                    this.container.bounding = [bounds.x, bounds.y, bounds.width, bounds.height];
                }
            }
        };
        
        // Calculate bounds between FanIn and FanOut nodes
        function calculateBoundsBetweenNodes(fanInNode, fanOutNode) {
            const margin = fanInNode.container.margin || 20;
            
            // Get all interconnected nodes between FanIn and FanOut
            const interconnectedNodes = findInterconnectedNodes(fanInNode, fanOutNode);
            
            // Initialize bounds calculator with positions of FanIn and FanOut nodes
            const bounds = {
                minX: Math.min(fanInNode.pos[0], fanOutNode.pos[0]),
                minY: Math.min(fanInNode.pos[1], fanOutNode.pos[1]),
                maxX: Math.max(
                    fanInNode.pos[0] + fanInNode.size[0], 
                    fanOutNode.pos[0] + fanOutNode.size[0]
                ),
                maxY: Math.max(
                    fanInNode.pos[1] + fanInNode.size[1], 
                    fanOutNode.pos[1] + fanOutNode.size[1]
                )
            };
            
            // Expand bounds to include all interconnected nodes
            for (const node of interconnectedNodes) {
                if (node === fanInNode || node === fanOutNode) continue;
                
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
        
        // Update contained nodes when graph is loaded/changed
        nodeType.prototype.recomputeInsideNodes = function() {
            if (!this.graph || !this.container) return;
            
            const fanOutNode = findFanInNode(this);
            if (!fanOutNode) return;
            
            this.container.nodes = [];
            
            // Calculate bounds between FanIn and FanOut
            const bounds = calculateBoundsBetweenNodes(this, fanOutNode);
            this.container.bounding = [bounds.x, bounds.y, bounds.width, bounds.height];
            
            // Check all nodes in the graph
            const allNodes = this.graph._nodes;
            if (!allNodes) return;
            
            for (const node of allNodes) {
                if (node === this || node === fanOutNode) continue; // Skip self and partner
                
                // Check if node is inside this container
                if (isNodeInsideContainer(node, this.container.bounding)) {
                    this.container.nodes.push(node);
                }
            }
        };
        
        // Check if a node is inside the container bounds
        function isNodeInsideContainer(node, bounds) {
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

        // Call recomputeInsideNodes after node has been configured
        const onConfigure = nodeType.prototype.onConfigure;
        nodeType.prototype.onConfigure = function(info) {
            const result = onConfigure ? onConfigure.call(this, info) : undefined;
            
            // Initialize container after loading
            if (info && this.container) {
                // Initialize from widget values if they exist
                if (this.widgets) {
                    const colorWidget = this.widgets.find(w => w.name === "color");
                    if (colorWidget) {
                        this.container.color = colorWidget.value;
                        this.color = colorWidget.value;
                    }
                    
                    const titleWidget = this.widgets.find(w => w.name === "title");
                    if (titleWidget) {
                        this.container.title = titleWidget.value;
                        this.title = titleWidget.value;
                    }
                }
                
                // Store initial position
                this.container.lastPos = [...this.pos];
                
                // Compute contained nodes after a short delay
                // to ensure all nodes are loaded
                setTimeout(() => {
                    this.recomputeInsideNodes();
                    if (this.graph) this.graph.setDirtyCanvas(true);
                }, 100);
            }
            
            return result;
        };
        
        // Register to mouse events to compute contained nodes when mouse is released
        const onMouseUp = nodeType.prototype.onMouseUp;
        nodeType.prototype.onMouseUp = function(e, pos, graphCanvas) {
            const result = onMouseUp ? onMouseUp.call(this, e, pos, graphCanvas) : undefined;
            
            // Re-compute contained nodes after mouse release
            this.recomputeInsideNodes();
            
            if (this.graph) this.graph.setDirtyCanvas(true);
            
            return result;
        };
        
        // Hook into the graph node added event
        if (app.graph && app.graph.add) {
            const originalAddNode = app.graph.add;
            app.graph.add = function(node) {
                const result = originalAddNode.apply(this, arguments);
                
                // Find all FanOut nodes and recompute
                if (this._nodes) {
                    for (const node of this._nodes) {
                        if (node && node.type === "ClusterFanOutImage") {
                            setTimeout(() => node.recomputeInsideNodes(), 100);
                        }
                    }
                }
                
                return result;
            };
        }
        
        // Hook into the graph node removed event
        if (app.graph && app.graph.remove) {
            const originalRemoveNode = app.graph.remove;
            app.graph.remove = function(node) {
                const result = originalRemoveNode.apply(this, arguments);
                
                // Find all FanOut nodes and recompute
                if (this._nodes) {
                    for (const node of this._nodes) {
                        if (node && node.type === "ClusterFanOutImage") {
                            setTimeout(() => node.recomputeInsideNodes(), 100);
                        }
                    }
                }
                
                return result;
            };
        }
        
        // Alternative approach: Use setTimeout to periodically check and update container nodes
        setInterval(() => {
            if (app.graph && app.graph._nodes) {
                for (const node of app.graph._nodes) {
                    if (node && node.type === "ClusterFanOutImage") {
                        node.recomputeInsideNodes();
                    }
                }
            }
        }, 5000); // Check every 5 seconds
    }
});