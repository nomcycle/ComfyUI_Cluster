from .base_nodes import SyncedNode
from ..env_vars import EnvVars


class ClusterInfo(SyncedNode):
    def __init__(self):
        super().__init__()

    @classmethod
    def INPUT_TYPES(s):
        return {}

    RETURN_TYPES = (
        "INT",
        "INT",
    )
    RETURN_NAMES = (
        "Instance Index",
        "Instance Count",
    )
    FUNCTION = "execute"
    CATEGORY = "Cluster"

    def execute(self):
        return (
            EnvVars.get_instance_index(),
            EnvVars.get_instance_count(),
        )
