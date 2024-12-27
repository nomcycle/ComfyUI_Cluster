from listener import UDPListener
from .protobuf.messages_pb2 import FenceRequest, FenceResponse

class ClusterNode:
    def __init__(self) -> None:
        self._listener = UDPListener(message_callback=self.handle_message)
    
    def handle_message(self, request: FenceRequest, addr) -> FenceResponse:
        raise NotImplementedError("Subclasses must implement handle_message")

class Leader(ClusterNode):
    def __init__(self) -> None:
        super().__init__()
    
    def handle_message(self, request: FenceRequest, addr) -> FenceResponse:
        response = FenceResponse()
        response.node_id = request.node_id
        # Add leader-specific handling logic here
        return response

class Follower(ClusterNode):
    def __init__(self) -> None:
        super().__init__()
    
    def handle_message(self, request: FenceRequest, addr) -> FenceResponse:
        response = FenceResponse()
        response.node_id = request.node_id
        # Add follower-specific handling logic here
        return response