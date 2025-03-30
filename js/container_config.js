// Configuration for container nodes in ComfyUI
export const NODE_CONTAINER_CONFIG = {
    // Subgraph container configuration
    subgraph: {
        startNodeType: "ClusterStartSubgraph",   // Type of the start node
        endNodeType: "ClusterEndSubgraph",       // Type of the end node
        margin: 40,                              // Margin around contained nodes
        color: "#5c7a44",                        // Default container color
        bgcolor: "#334433",                      // Default background color
        title: "Cluster Workflow",               // Default title
    },
    // Fan container configuration
    fan: {
        startNodeType: "ClusterFanOutImage",     // Type of the start node
        endNodeType: "ClusterFanInImages",       // Type of the end node
        margin: 40,                              // Margin around contained nodes
        color: "#4477aa",                        // Default container color
        bgcolor: "#334433",                      // Default background color
        title: "Fan Container",                  // Default title
    },
    // Add other specific configurations as needed
    // Example for latent operations
    latent: {
        startNodeType: "ClusterFanOutLatent",    // Type of the start node
        endNodeType: "ClusterGatherLatents",     // Type of the end node
        margin: 40,                              // Margin around contained nodes
        color: "#994477",                        // Default container color
        bgcolor: "#443344",                      // Default background color
        title: "Latent Container",               // Default title
    },
    // Example for mask operations
    mask: {
        startNodeType: "ClusterFanOutMask",      // Type of the start node
        endNodeType: "ClusterGatherMasks",       // Type of the end node
        margin: 40,                              // Margin around contained nodes
        color: "#779944",                        // Default container color
        bgcolor: "#344433",                      // Default background color
        title: "Mask Container",                 // Default title
    }
};