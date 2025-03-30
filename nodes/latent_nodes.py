import torch
from .tensor_nodes import ClusterGatherBase, ClusterFanOutBase, ClusterFanInBase, declare_subgraph_end_node, declare_subgraph_start_node


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

@declare_subgraph_start_node('latent_fan')
class ClusterFanOutLatent(ClusterLatentNodeMixin, ClusterFanOutBase):
    pass

@declare_subgraph_end_node('latent_fan')
class ClusterFanInLatent(ClusterLatentNodeMixin, ClusterFanInBase):
    pass
