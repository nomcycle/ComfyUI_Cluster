import torch
import traceback
from .tensor_nodes import ClusterFanInBase, ClusterGatherBase, ClusterFanOutBase, ClusterBroadcastTensor, declare_subgraph_end_node, declare_subgraph_start_node
from ..env_vars import EnvVars
from ..log import logger

# Import LoadImage from the correct location
from nodes import LoadImage


class ClusterImageNodeMixin:
    INPUT_TYPE = "IMAGE"
    RETURN_TYPES = ("IMAGE",)

    def get_input(self, input) -> torch.Tensor:
        return input[0] if isinstance(input, tuple) else input

@declare_subgraph_end_node('image_fan')
class ClusterFanInImages(ClusterImageNodeMixin, ClusterFanInBase):
    pass

class ClusterGatherImages(ClusterImageNodeMixin, ClusterGatherBase):
    pass

@declare_subgraph_start_node('image_fan')
class ClusterFanOutImage(ClusterImageNodeMixin, ClusterFanOutBase):
    pass

class ClusterBroadcastLoadedImage(ClusterBroadcastTensor):
    @classmethod
    def INPUT_TYPES(s):
        input_types = LoadImage.INPUT_TYPES()
        if "hidden" not in input_types:
            input_types["hidden"] = {}
        input_types["hidden"]["unique_id"] = "UNIQUE_ID"
        return input_types

    RETURN_TYPES = ("IMAGE",)

    def get_input(self, image) -> torch.Tensor:
        if EnvVars.get_instance_index() == 0:
            image, _ = LoadImage().load_image(image)
            return image
        return None

    def execute(self, image, unique_id):
        try:
            output = self.blocking_sync(self.get_input(image), unique_id)
            return self._prepare_output(output)
        except Exception as e:
            logger.error(
                "Error executing tensor operation: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            raise e

    @classmethod
    def IS_CHANGED(s, image):
        if EnvVars.get_instance_index() == 0:
            return LoadImage.IS_CHANGED(image)
        return True

    @classmethod
    def VALIDATE_INPUTS(s, image):
        if EnvVars.get_instance_index() == 0:
            return LoadImage.VALIDATE_INPUTS(image)
        return True

    async def _sync_operation(self, instance, input):
        if EnvVars.get_instance_index() == 0:
            return await instance._this_instance.broadcast_tensor(input)
        return await instance._this_instance.receive_tensor_fanout()
