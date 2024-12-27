import socket
import asyncio
import json

from google.protobuf.json_format import ParseDict
from .protobuf.messages_pb2 import FenceRequest, FenceResponse

class UDPListener:
    def __init__(self, host='0.0.0.0', port=6677, message_callback=None):
        self.host = host
        self.port = port
        self.loop = asyncio.get_event_loop()
        self.transport = None
        self.message_callback = message_callback

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode())
            request = ParseDict(message, FenceRequest())
            
            if self.message_callback:
                response = self.message_callback(request, addr)
            else:
                response = FenceResponse()
                response.node_id = request.node_id
            
            self.transport.sendto(MessageToJson(response).encode(), addr)
            
        except Exception as e:
            print(f"Error processing message: {e}")