import asyncio
import queue
import threading
import time
from abc import ABC, abstractmethod

from .sender import UDPEmitter
from .listener import UDPListener
from .log import logger
from .env_vars import EnvVars
from .queued import IncomingPacket

class UDPSingleton:
    _listener: UDPListener = None

    _incoming_thread = None
    _receive_async_loop = None
    _outgoing_thread = None
    _running: bool = False

    _incoming_thread_callbacks = []
    _outgoing_thread_callbacks = []
    _cluster_instance_addressses: [(int, str)] = []
    _message_index: int = 0
    _incoming_queue: queue.Queue = queue.Queue()

    @classmethod
    def start_threads(cls):
        if not cls._running:

            cls._running = True
            cls._incoming_thread = threading.Thread(target=cls._incoming_thread_fn, daemon=True)
            cls._incoming_thread.name = 'ComfyCluster-IncomingPackets'

            cls._outgoing_thread = threading.Thread(target=cls._outgoing_thread_fn, daemon=True)
            cls._outgoing_thread.name = 'ComfyCluster-OutgoingPackets'

            cls._incoming_thread.start()
            cls._outgoing_thread.start()

    @classmethod
    def stop_threads(cls):
        cls._running = False
        if cls._incoming_thread:
            cls._incoming_thread.join()
        if cls._outgoing_thread:
            cls._outgoing_thread.join()

    @classmethod
    def set_cluster_instance_addresses(cls, addresses: [(int, str)]):
        logger.info("Setting cluster addresses: %s", addresses)
        cls._cluster_instance_addressses = addresses

    @classmethod
    def get_cluster_instance_address(cls, instance_id: int) -> str:
        return cls._cluster_instance_addressses[instance_id][1]
    
    @classmethod
    def get_cluster_instance_addresses(cls):
        return cls._cluster_instance_addressses

    @classmethod
    def iterate_message_id(cls):
        cls._message_index += 1
        return cls._message_index

    @classmethod
    def add_handle_incoming_packet_callback(cls, incoming_callback):
        cls._incoming_thread_callbacks.append(incoming_callback)

    @classmethod 
    def add_outgoing_thread_callback(cls, outgoing_callback):
        cls._outgoing_thread_callbacks.append(outgoing_callback)

    @classmethod 
    def _dequeue_packet(cls):
        incoming_packet = cls._incoming_queue.get()
        return

    @classmethod 
    def _incoming_thread_fn(cls):
        logger.info("Starting incoming thread.")
        cls._listener = UDPListener(EnvVars.get_listen_address(), EnvVars.get_listen_port())
        packet_count = 0
        
        while cls._running:
            packet, sender_addr = cls._listener.poll()
            incoming_packet = IncomingPacket(packet, sender_addr)
            for callback in cls._incoming_thread_callbacks:
                try:
                    callback(incoming_packet)
                except Exception as e:
                    logger.error(f"Error in receive callback: {e}")
            cls._incoming_queue.put(IncomingPacket(packet, sender_addr))
            
            packet_count += 1
            if packet_count >= 1000:
                logger.info(f"Processed {packet_count} incoming packets")
                packet_count = 0

        logger.info("Exited incoming thread.")
    
    @classmethod
    def _outgoing_thread_fn(cls):
        while cls._running:
            for callback in cls._outgoing_thread_callbacks:
                try:
                    start_time = time.time()
                    callback()
                    elapsed_time = time.time() - start_time
                    if elapsed_time > 0.01:  # Only log if above 10ms
                        logger.debug(f"Outgoing callback took {elapsed_time:.3f} seconds")
                except Exception as e:
                    logger.error(f"Error in send callback: {e}")

    @classmethod
    def process_batch_outgoing(cls, outgoing_queue: queue.Queue, emit_fn):
        if outgoing_queue.empty():
            return
        
        while not outgoing_queue.empty():
            queued_item = outgoing_queue.get()
            emit_fn(queued_item)
            outgoing_queue.task_done()
            time.sleep(0.0001)
            
    
class ACKResult:
    def __init__(self, success: bool, error_msg: str | None = None):
        self.success = success
        self.error_msg = error_msg

class UDPBase(ABC):
    def __init__(self, incoming_processed_packet_queue):
        self._outgoing_queue: queue.Queue = queue.Queue()
        self._incoming_processed_packet_queue = incoming_processed_packet_queue
        self._emitter = UDPEmitter(EnvVars.get_send_port())

    @abstractmethod
    def _handle_incoming_packet(self, packet, sender_addr: str):
        """Abstract method for handling received messages"""
    
    @abstractmethod 
    def _outgoing_thread_callback(self):
        """Abstract method for handling message sending"""