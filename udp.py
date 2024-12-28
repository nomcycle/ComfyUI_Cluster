import socket
import json
import threading
import time
import queue
from google.protobuf.json_format import MessageToJson
from .log import logger

from .sender import UDPSender
from .listener import UDPListener
from .protobuf.messages_pb2 import *
from google.protobuf.json_format import ParseDict

class UDP:
    def __init__(self, on_receive_message_callback):
        self._listener = UDPListener(message_callback=self._handle_message)
        self._sender = UDPSender()

        self._message_queue = queue.Queue()
        self._message_index = 0

        self.on_receive_message_callback = on_receive_message_callback

        self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._running = True
        
        self._send_thread.start()
        self._receive_thread.start()

        self._propagate_hot_reload_signal()

    def _propagate_hot_reload_signal (self):
        self._hot_reload_timestamp = time.time()
        signal_hot_reload = ClusterSignalHotReload()
        signal_hot_reload.type = ClusterMessageType.SIGNAL_HOT_RELOAD
        signal_hot_reload.timestamp = str(time.time())
        self.send(signal_hot_reload)

    def _iterate_message_id(self):
        self._message_index = self._message_index + 1
        return self._message_index
    
    def send(self, message):
        self._iterate_message_id()
        message.message_id = self._message_index
        self._message_queue.put(message)

    def _handle_message(self, message, addr):
        msg_type = ClusterMessageType.Value(message.get('type'))
        if msg_type == ClusterMessageType.ACK:
            self._handle_ack(message)
            return
        elif self.on_receive_message_callback is not None:
            self.on_receive_message_callback(msg_type, message, addr)

    def _receive_loop(self):
        while self._running:
            self._listener.poll()

    def _send_loop(self):
        while self._running:
            # Send any queued messages
            try:
                while True:  # Process all available messages
                    message = self._message_queue.get_nowait()
                    self._sender.send(message)
                    self._message_queue.task_done()
            except queue.Empty:
                pass  # Queue is empty, continue with announce

    def _handle_ack(self, message):
        ack = ParseDict(message, ClusterAck())
