from ..instance_loop import InstanceLoop, get_instance_loop


class Anything(str):
    def __ne__(self, _):
        return False


class SyncedNode:
    instance: InstanceLoop = get_instance_loop()
    node_count = 0

    def __init__(self):
        SyncedNode.node_count += 1
        self._node_instance_id = SyncedNode.node_count
