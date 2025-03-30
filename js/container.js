// Container Node implementation for ComfyUI
import { app } from "../../../scripts/app.js";
import { NODE_CONTAINER_CONFIG } from "./container_config.js";
import { findEndNode, findInterconnectedNodes } from "./container_utils.js";
import { drawContainer, recalculateContainerNodes } from "./container_renderer.js";

// Register node for the Container Node system
app.registerExtension({
    name: "ComfyUI.Cluster.ContainerNodes",

    async beforeRegisterNodeDef(nodeType, nodeData, app) {
        // Check if this is a container start node type
        const nodeTypeName = nodeData.name;
        const isStartNode = Object.values(NODE_CONTAINER_CONFIG)
            .some(config => config.startNodeType === nodeTypeName);
        
        // Only apply to container start nodes
        if (!isStartNode) {
            return;
        }
        
        // Find the matching configuration
        const config = Object.values(NODE_CONTAINER_CONFIG)
            .find(config => config.startNodeType === nodeTypeName);
        
        if (!config) {
            console.warn(`No container configuration found for node type: ${nodeTypeName}`);
            return;
        }

        // Initialize the node execution handler
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

        // Override the node constructor to add container behavior
        const onNodeCreated = nodeType.prototype.onNodeCreated;
        nodeType.prototype.onNodeCreated = function() {
            const result = onNodeCreated ? onNodeCreated.apply(this, arguments) : undefined;
            
            // Create container properties with config values
            this.container = {
                nodes: [],                          // Array of nodes inside this container
                margin: config.margin || 40,        // Margin around contained nodes
                color: config.color || "#5c7a44",   // Default container color
                title: config.title || "Container", // Default title
                isContainer: true,                  // Flag to identify as container
                bounding: [0, 0, 300, 200],         // Default size [x, y, width, height]
                lastPos: [0, 0],                    // Track last position for movement
                endNodeType: config.endNodeType     // Store the end node type
            };
            
            // Set custom size for this node
            this.size = [300, 200];
            this.bgcolor = config.bgcolor || "#334433";
            this.color = config.color || "#5c7a44";

            // Store original position for move calculations
            this.container.lastPos = [...this.pos];
            
            return result;
        };
        
        // Add custom drawing method for the container
        nodeType.prototype.onDrawBackground = function(ctx, canvas) {
            if (!this.flags || !this.flags.collapsed) {
                // Find the corresponding end node using the stored end node type
                const endNode = findEndNode(this, this.container.endNodeType);
                
                // Only draw container if we have an end node
                if (endNode) {
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
                    
                    // Draw the container
                    drawContainer(this, endNode, ctx);
                }
            }
        };
        
        // Method to recompute contained nodes
        nodeType.prototype.recomputeInsideNodes = function() {
            if (!this.graph || !this.container) return;
            
            const endNode = findEndNode(this, this.container.endNodeType);
            if (!endNode) return;
            
            recalculateContainerNodes(this, endNode);
        };

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
        
        // Set up graph event handlers for node changes
        this.setupGraphEventHandlers();
        
        // Set up periodic refresh to ensure containers stay up to date
        this.setupPeriodicRefresh();
    },
    
    // Set up graph event handlers for node additions and removals
    setupGraphEventHandlers() {
        if (!app.graph) return;
        
        // Hook into the graph node added event
        if (app.graph.add && !app.graph._containerNodeAddHooked) {
            const originalAddNode = app.graph.add;
            app.graph.add = function(node) {
                const result = originalAddNode.apply(this, arguments);
                
                // Find all container start nodes and recompute
                if (this._nodes) {
                    for (const node of this._nodes) {
                        if (node && node.container && node.container.isContainer) {
                            setTimeout(() => node.recomputeInsideNodes(), 100);
                        }
                    }
                }
                
                return result;
            };
            app.graph._containerNodeAddHooked = true;
        }
        
        // Hook into the graph node removed event
        if (app.graph.remove && !app.graph._containerNodeRemoveHooked) {
            const originalRemoveNode = app.graph.remove;
            app.graph.remove = function(node) {
                const result = originalRemoveNode.apply(this, arguments);
                
                // Find all container start nodes and recompute
                if (this._nodes) {
                    for (const node of this._nodes) {
                        if (node && node.container && node.container.isContainer) {
                            setTimeout(() => node.recomputeInsideNodes(), 100);
                        }
                    }
                }
                
                return result;
            };
            app.graph._containerNodeRemoveHooked = true;
        }
    },
    
    // Set up a periodic refresh to keep container bounds updated
    setupPeriodicRefresh() {
        if (!this._containerRefreshInterval) {
            this._containerRefreshInterval = setInterval(() => {
                if (app.graph && app.graph._nodes) {
                    for (const node of app.graph._nodes) {
                        if (node && node.container && node.container.isContainer) {
                            node.recomputeInsideNodes();
                        }
                    }
                }
            }, 5000); // Check every 5 seconds
        }
    }
});