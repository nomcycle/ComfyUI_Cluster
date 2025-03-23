import torch
from comfy.comfy_types import IO, ComfyNodeABC, InputTypeDict

class SimpleVisualNode(ComfyNodeABC):

    @classmethod
    def INPUT_TYPES(s) -> InputTypeDict:
        return {
            "required": {
                "image": ("IMAGE",),
            },
        }
    
    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"
    
    def execute(self, image):
        return (input,)