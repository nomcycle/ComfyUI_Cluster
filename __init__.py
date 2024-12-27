from .protobuf.messages_pb2 import FenceRequest, FenceResponse

from .cluster_node import ClusterNode

class SyncedNode:
    instance_count = 0

    def __init__(self):
        SyncedNode.instance_count += 1
        self._instance_id = SyncedNode.instance_count

class FenceClusteredWorkflow(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {
                "required": { "anything": ("*",{})},
            }
        }

    RETURN_TYPES = ("",)
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self):
        return

NODE_CLASS_MAPPINGS = {
    "FenceClusteredWorkflow": FenceClusteredWorkflow
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "FenceClusteredWorkflow": "Sync Clustered Workflow"
}