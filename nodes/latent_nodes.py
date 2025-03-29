import torch
from .tensor_nodes import ClusterGatherBase, ClusterFanOutBase


class ClusterLatentNodeMixin:
    INPUT_TYPE = "LATENT"
    RETURN_TYPES = ("LATENT",)

    def get_input(self, input) -> torch.Tensor:
        samples = input["samples"]
        if len(samples.shape) == 4 and samples.shape[0] == 1:
            samples = samples.squeeze(0)
        return samples

    def _prepare_output(self, output):
        return ({"samples": output},)


class ClusterGatherLatents(ClusterLatentNodeMixin, ClusterGatherBase):
    pass


class ClusterFanOutLatent(ClusterLatentNodeMixin, ClusterFanOutBase):
    pass
