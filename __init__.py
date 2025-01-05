from .instance_loop import InstanceLoop, create_instance_loop
from .protobuf.messages_pb2 import ClusterRole

class SyncedNode:

    node_count = 0
    node: InstanceLoop = create_instance_loop()

    def __init__(self):
        SyncedNode.node_count += 1
        self._node_instance_id = SyncedNode.node_count

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