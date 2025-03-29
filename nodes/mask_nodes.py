import torch
from .tensor_nodes import ClusterGatherBase, ClusterFanOutBase


class ClusterMaskNodeMixin:
    INPUT_TYPE = "MASK"
    RETURN_TYPES = ("MASK",)

    def get_input(self, input) -> torch.Tensor:
        return input[0]


class ClusterGatherMasks(ClusterMaskNodeMixin, ClusterGatherBase):
    pass


class ClusterFanOutMask(ClusterMaskNodeMixin, ClusterFanOutBase):
    pass
