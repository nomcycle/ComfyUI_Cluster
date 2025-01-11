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
            cls._outgoing_thread = threading.Thread(target=cls._outgoing_thread_fn, daemon=True)

            cls._incoming_thread.start()
            cls._outgoing_thread.start()

            cls._handle_incoming_packet_thread = threading.Thread(target=cls._handle_incoming_packet_thread_fn, daemon=True)
            cls._handle_incoming_packet_thread.start()

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
    def _handle_incoming_packet_thread_fn(cls):
        cls._receive_async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls._receive_async_loop)
        cls._receive_async_loop.run_until_complete(cls._handle_incoming_packet_loop())
        cls._receive_async_loop.close()

    @classmethod 
    async def _handle_incoming_packet_loop(cls):
        logger.info("Starting handle packet async loop.")
        cls._listener = UDPListener(EnvVars.get_listen_address(), EnvVars.get_listen_port())
        while cls._running:
            incoming_packet = cls._incoming_queue.get()
            for callback in cls._incoming_thread_callbacks:
                try:
                    await callback(incoming_packet)
                except Exception as e:
                    logger.error(f"Error in receive callback: {e}")
            await asyncio.sleep(0.001)
        logger.info("Exited handle packet async loop.")

    @classmethod 
    def _incoming_thread_fn(cls):
        logger.info("Starting incoming thread.")
        cls._listener = UDPListener(EnvVars.get_listen_address(), EnvVars.get_listen_port())
        while cls._running:
            packet, sender_addr = cls._listener.poll()
            cls._incoming_queue.put(IncomingPacket(packet, sender_addr))
        logger.info("Exited incoming thread.")
    
    @classmethod
    def _outgoing_thread_fn(cls):
        while cls._running:
            for callback in cls._outgoing_thread_callbacks:
                try:
                    callback()
                except Exception as e:
                    logger.error(f"Error in send callback: {e}")
            time.sleep(0.001)

    @classmethod
    def process_batch_outgoing(cls, queue, emit_fn, batch_size=100):
        if queue.empty():
            return

        start_time = time.time()
        total_items = 0
        
        while not queue.empty():
            # Process up to batch_size items as fast as possible
            batch_count = 0
            batch_start = time.time()
            
            while batch_count < batch_size and not queue.empty():
                queued_item = queue.get()
                emit_fn(queued_item)
                queue.task_done()
                batch_count += 1
                total_items += 1
                
            # Only sleep between batches, not packets
            batch_duration = time.time() - batch_start
            # logger.debug("Batch processing took %.3fms", batch_duration * 1000)
            
            time.sleep(max(0.0001 - batch_duration, 0.0))
    
class ACKResult:
    def __init__(self, success: bool, error_msg: str = None):
        self.success = success
        self.error_msg = error_msg

class UDPBase(ABC):
    def __init__(self):
        self._outgoing_queue: queue.Queue = queue.Queue()
        self._emitter = UDPEmitter(EnvVars.get_send_port())

    @abstractmethod
    async def _handle_incoming_packet(self, packet, sender_addr: str):
        """Abstract method for handling received messages"""
    
    @abstractmethod 
    def _outgoing_thread_callback(self):
        """Abstract method for handling message sending"""