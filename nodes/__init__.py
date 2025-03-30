"""
ComfyUI Cluster nodes module.
This module contains nodes for cluster operations in ComfyUI.
"""

# Info nodes
from .info_nodes import ClusterInfo

# Memory management nodes
from .memory_nodes import ClusterFinallyFree, ClusterFreeNow

# Workflow execution nodes
from .workflow_nodes import ClusterExecuteWorkflow, ClusterExecuteCurrentWorkflow, ClusterStartSubgraph, ClusterEndSubgraph, ClusterUseSubgraph

# Tensor operation nodes
from .tensor_nodes import (
    ClusterTensorNodeBase,
    ClusterFanInBase,
    ClusterGatherBase,
    ClusterFanOutBase,
    ClusterListenTensorBroadcast,
    ClusterBroadcastTensor,
)

# Image processing nodes
from .image_nodes import (
    ClusterFanInImages,
    ClusterGatherImages,
    ClusterFanOutImage,
    ClusterBroadcastLoadedImage,
)

# Latent processing nodes
from .latent_nodes import ClusterGatherLatents, ClusterFanOutLatent

# Mask processing nodes
from .mask_nodes import ClusterGatherMasks, ClusterFanOutMask

# Batch processing nodes
from .batch_nodes import (
    ClusterGetInstanceWorkItemFromBatch,
    ClusterFlattenBatchedImageList,
    ClusterSplitBatchToList,
    ClusterInsertAtIndex,
    ClusterStridedReorder,
)

# Register all nodes
NODE_CLASS_MAPPINGS = {
    "ClusterInfo": ClusterInfo,
    "ClusterFinallyFree": ClusterFinallyFree,
    "ClusterFreeNow": ClusterFreeNow,
    "ClusterGetInstanceWorkItemFromBatch": ClusterGetInstanceWorkItemFromBatch,
    "ClusterExecuteWorkflow": ClusterExecuteWorkflow,
    "ClusterStartSubgraph": ClusterStartSubgraph,
    "ClusterEndSubgraph": ClusterEndSubgraph,
    "ClusterUseSubgraph": ClusterUseSubgraph,
    "ClusterExecuteCurrentWorkflow": ClusterExecuteCurrentWorkflow,
    "ClusterListenTensorBroadcast": ClusterListenTensorBroadcast,
    "ClusterBroadcastTensor": ClusterBroadcastTensor,
    "ClusterBroadcastLoadedImage": ClusterBroadcastLoadedImage,
    "ClusterFanInImages": ClusterFanInImages,
    "ClusterGatherImages": ClusterGatherImages,
    "ClusterGatherLatents": ClusterGatherLatents,
    "ClusterGatherMasks": ClusterGatherMasks,
    "ClusterFanOutImage": ClusterFanOutImage,
    "ClusterFanOutLatent": ClusterFanOutLatent,
    "ClusterFanOutMask": ClusterFanOutMask,
    "ClusterFlattenBatchedImageList": ClusterFlattenBatchedImageList,
    "ClusterSplitBatchToList": ClusterSplitBatchToList,
    "ClusterInsertAtIndex": ClusterInsertAtIndex,
    "ClusterStridedReorder": ClusterStridedReorder,
}

# Register node display names
NODE_DISPLAY_NAME_MAPPINGS = {
    "ClusterInfo": "Cluster Info",
    "ClusterFinallyFree": "Cluster Finally Free",
    "ClusterFreeNow": "Cluster Free Now",
    "ClusterGetInstanceWorkItemFromBatch": "Cluster Get Instance Work Item From Batch",
    "ClusterExecuteWorkflow": "Cluster Execute Workflow",
    "ClusterStartSubgraph": "Cluster Start Subgraph",
    "ClusterEndSubgraph": "Cluster End Subgraph",
    "ClusterUseSubgraph": "Cluster Use Subgraph",
    "ClusterExecuteCurrentWorkflow": "Cluster Execute Current Workflow",
    "ClusterListenTensorBroadcast": "Cluster Listen Tensor Broadcast",
    "ClusterBroadcastTensor": "Cluster Broadcast Tensor",
    "ClusterBroadcastLoadedImage": "Cluster Broadcast Loaded Image",
    "ClusterFanInImages": "Cluster Fan In Images",
    "ClusterGatherImages": "Cluster Gather Images",
    "ClusterGatherLatents": "Cluster Gather Latents",
    "ClusterGatherMasks": "Cluster Gather Masks",
    "ClusterFanOutImage": "Cluster Fan Out Image",
    "ClusterFanOutLatent": "Cluster Fan Out Latent",
    "ClusterFanOutMask": "Cluster Fan Out Mask",
    "ClusterFlattenBatchedImageList": "Cluster Flatten Batched Image List",
    "ClusterSplitBatchToList": "Cluster Split Batch To List",
    "ClusterInsertAtIndex": "Cluster Insert At Index",
    "ClusterStridedReorder": "Cluster Strided Reorder",
}
