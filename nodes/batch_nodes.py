import torch

from .base_nodes import SyncedNode
from ..env_vars import EnvVars
from ..log import logger


class ClusterGetInstanceWorkItemFromBatch(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {"required": {"images": ("IMAGE",)}}

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self, images):
        instance_index = EnvVars.get_instance_index()
        instance_count = EnvVars.get_instance_count()
        batch_size = images.shape[0]

        if instance_count <= 1:
            return (images,)

        # Calculate slice for this instance
        base_size = batch_size // instance_count
        remainder = batch_size % instance_count
        start_idx = instance_index * base_size + min(instance_index, remainder)
        end_idx = (
            start_idx + base_size + (1 if instance_index < remainder else 0)
        )

        # Handle edge cases
        if start_idx >= batch_size or start_idx >= end_idx:
            return (
                torch.zeros(
                    (1, *images.shape[1:]),
                    dtype=images.dtype,
                    device=images.device,
                ),
            )

        logger.info(
            f"Instance {instance_index}/{instance_count} processing {end_idx - start_idx} images ({start_idx}-{end_idx - 1})"
        )
        return (images[start_idx:end_idx].clone(),)


class ClusterFlattenBatchedImageList(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "images": ("IMAGE",),
            }
        }

    INPUT_IS_LIST = True
    RETURN_TYPES = ("IMAGE",)
    OUTPUT_IS_LIST = (True,)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, images):
        # Flatten a list of batches into a list of individual images
        flattened_images = []
        for batch in images:
            for i in range(batch.shape[0]):
                flattened_images.append(batch[i : i + 1])

        return (flattened_images,)


class ClusterSplitBatchToList(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "image": ("IMAGE",),
                "images_per_list_element": (
                    "INT",
                    {"default": 2, "min": 1, "max": 64},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    OUTPUT_IS_LIST = (True,)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, image, images_per_list_element):
        batch_size = image.shape[0]

        # Handle edge case: if batch size is smaller than requested images per element
        if batch_size <= images_per_list_element:
            return ([image],)

        # Calculate how many list elements we need
        num_elements = (
            batch_size + images_per_list_element - 1
        ) // images_per_list_element

        result = []
        for i in range(num_elements):
            start_idx = i * images_per_list_element
            end_idx = min(start_idx + images_per_list_element, batch_size)
            result.append(image[start_idx:end_idx])

        return (result,)


class ClusterInsertAtIndex(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "batch": ("IMAGE",),
                "images_to_insert": ("IMAGE",),
                "insert_at_index": (
                    "INT",
                    {"default": 0, "min": 0, "max": 10000},
                ),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, batch, images_to_insert, insert_at_index):
        # Ensure batch is treated as a batch even if it's a single image
        if len(batch.shape) == 3:  # Single image with shape [H, W, C]
            batch = batch.unsqueeze(0)  # Convert to [1, H, W, C]

        batch_size = batch.shape[0]

        # Ensure insert_at_index is within valid range
        insert_at_index = min(insert_at_index, batch_size)

        # Split the batch at the insertion point
        first_part = batch[:insert_at_index]
        second_part = batch[insert_at_index:]

        # Concatenate the parts with the images to insert in between
        result = torch.cat([first_part, images_to_insert, second_part], dim=0)

        return (result,)


class ClusterStridedReorder(SyncedNode):
    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "image": ("IMAGE",),
                "stride": ("INT", {"default": 2, "min": 1, "max": 128}),
            }
        }

    RETURN_TYPES = ("IMAGE",)
    FUNCTION = "execute"
    CATEGORY = "Cluster/Image"

    def execute(self, image, stride):
        batch_size = image.shape[0]

        # Handle edge cases
        if batch_size <= 1 or stride <= 1:
            return (image,)

        # Original indices
        indices = list(range(batch_size))
        new_order = []

        # Apply strided reordering pattern:
        # For each position within the stride, collect all elements at that position
        for pos in range(stride):
            # Get indices at position pos, pos+stride, pos+2*stride, etc.
            group_indices = indices[pos::stride]
            new_order.extend(group_indices)

        # Reorder the batch according to the pattern
        reordered_images = torch.stack(
            [image[idx] for idx in new_order], dim=0
        )
        return (reordered_images,)
