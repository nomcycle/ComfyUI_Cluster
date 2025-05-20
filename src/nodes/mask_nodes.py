import torch
from .tensor_nodes import ClusterGatherBase, ClusterFanOutBase, ClusterFanInBase, declare_subgraph_end_node, declare_subgraph_start_node


class ClusterMaskNodeMixin:
    INPUT_TYPE = "MASK"
    RETURN_TYPES = ("MASK",)

    def get_input(self, input) -> torch.Tensor:
        return input[0]


class ClusterGatherMasks(ClusterMaskNodeMixin, ClusterGatherBase):
    pass


@declare_subgraph_start_node('mask_fan')
class ClusterFanOutMask(ClusterMaskNodeMixin, ClusterFanOutBase):
    pass

@declare_subgraph_end_node('mask_fan')
class ClusterFanInMask(ClusterMaskNodeMixin, ClusterFanInBase):
    pass
