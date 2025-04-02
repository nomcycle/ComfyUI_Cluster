import queue
import threading
import time
import traceback
from abc import ABC, abstractmethod
from typing import Callable, Optional, List, Tuple, Dict, Any

from ..log import logger
from ..env_vars import EnvVars
from .sender import UDPEmitter
from .listener import UDPListener
from .queued import IncomingPacket, IncomingBuffer, OutgoingPacket


class ThreadManager:
    """
    Manages the creation, starting, and stopping of threads for UDP communication.
    """
    def __init__(self):
        self._incoming_thread = None
        self._outgoing_thread = None
        self._running: bool = False
        self._incoming_thread_callbacks: List[Callable] = []
        self._outgoing_thread_callbacks: List[Callable] = []
    
    def start_threads(self, incoming_fn: Callable, outgoing_fn: Callable):
        """Start the incoming and outgoing threads."""
        if not self._running:
            self._running = True
            self._incoming_thread = threading.Thread(
                target=incoming_fn, daemon=True
            )
            self._incoming_thread.name = "ComfyCluster-IncomingPackets"

            self._outgoing_thread = threading.Thread(
                target=outgoing_fn, daemon=True
            )
            self._outgoing_thread.name = "ComfyCluster-OutgoingPackets"

            self._incoming_thread.start()
            self._outgoing_thread.start()
            
    def stop_threads(self):
        """Stop the incoming and outgoing threads."""
        self._running = False
        if self._incoming_thread:
            self._incoming_thread.join()
        if self._outgoing_thread:
            self._outgoing_thread.join()
    
    def is_running(self) -> bool:
        """Check if threads are running."""
        return self._running
    
    def add_incoming_callback(self, callback: Callable):
        """Add a callback function to be called for each incoming packet."""
        self._incoming_thread_callbacks.append(callback)
    
    def add_outgoing_callback(self, callback: Callable):
        """Add a callback function to be called for processing outgoing packets."""
        self._outgoing_thread_callbacks.append(callback)
    
    def get_incoming_callbacks(self) -> List[Callable]:
        """Get the list of incoming packet callbacks."""
        return self._incoming_thread_callbacks
    
    def get_outgoing_callbacks(self) -> List[Callable]:
        """Get the list of outgoing packet callbacks."""
        return self._outgoing_thread_callbacks


class AddressRegistry:
    """
    Manages the registry of cluster instance addresses.
    """
    def __init__(self):
        self._cluster_instance_addresses: List[Tuple[int, str, int]] = []
    
    def set_addresses(self, addresses: List[Tuple[int, str, int]]):
        """Set the addresses of all cluster instances."""
        logger.info("Setting cluster addresses: %s", addresses)
        self._cluster_instance_addresses = addresses
    
    def get_address(self, instance_id: int) -> Tuple[str, int]:
        """Get the address of a specific instance."""
        return (
            self._cluster_instance_addresses[instance_id][1],
            self._cluster_instance_addresses[instance_id][2],
        )
    
    def get_all_addresses(self) -> List[Tuple[int, str, int]]:
        """Get all cluster instance addresses."""
        return self._cluster_instance_addresses


class MessageIdGenerator:
    """
    Generates unique message IDs for outgoing messages.
    """
    def __init__(self):
        self._message_index: int = 0
        self._counter_lock = threading.Lock()
    
    def next_id(self) -> int:
        """Generate the next message ID."""
        with self._counter_lock:
            self._message_index += 1
            return self._message_index


class PacketProcessor:
    """
    Processes incoming and outgoing packets.
    """
    def __init__(self, thread_manager: ThreadManager):
        self._thread_manager = thread_manager
        self._broadcast_listener: Optional[UDPListener] = None
        self._direct_listener: Optional[UDPListener] = None
    
    def setup_listeners(self):
        """Set up the broadcast and direct listeners."""
        self._broadcast_listener = UDPListener(
            EnvVars.get_listen_address(), EnvVars.get_broadcast_port()
        )
        self._direct_listener = UDPListener(
            EnvVars.get_listen_address(), EnvVars.get_direct_listen_port()
        )
    
    def incoming_thread_fn(self):
        """Function run by the incoming thread to process incoming packets."""
        logger.info("Starting incoming thread.")
        self.setup_listeners()
        step_packet_count = 0
        packet_count = 0

        while self._thread_manager.is_running():
            packet, sender_addr = self._broadcast_listener.poll()
            if packet is None or sender_addr is None:
                packet, sender_addr = self._direct_listener.poll()
                if packet is None or sender_addr is None:
                    time.sleep(0.001)
                    continue

            if IncomingPacket.is_buffer(packet):
                incoming = IncomingBuffer(packet, sender_addr)
            else:
                incoming = IncomingPacket(packet, sender_addr)

            for callback in self._thread_manager.get_incoming_callbacks():
                try:
                    callback(incoming)
                except Exception as e:
                    logger.error(
                        f"Error in receive callback: {e}\n{traceback.format_exc()}"
                    )

            packet_count += 1
            if packet_count >= step_packet_count + 1000:
                step_packet_count = packet_count

        logger.info("Exited incoming thread.")
    
    def outgoing_thread_fn(self):
        """Function run by the outgoing thread to process outgoing packets."""
        while self._thread_manager.is_running():
            for callback in self._thread_manager.get_outgoing_callbacks():
                try:
                    start_time = time.time()
                    callback()
                    elapsed_time = time.time() - start_time
                    if elapsed_time > 0.1:  # Only log if above 10ms
                        logger.debug(
                            f'Outgoing callback: "{callback.__module__}.{callback.__name__}" took {elapsed_time:.3f} seconds'
                        )
                except Exception as e:
                    logger.error(
                        f"Error in send callback: {e}\n{traceback.format_exc()}"
                    )
    
    def process_batch_outgoing(self, dequeue_fn: Callable, emit_fn: Callable):
        """Process a batch of outgoing packets."""
        # Buffer size is 262144, MTU is typically 1500 bytes
        # So we can batch around 174 packets at a time
        batch_size = 174
        count = 0

        while count < batch_size:
            # Fast batch collection
            packet = dequeue_fn()
            if packet is None:
                break
            try:
                emit_fn(packet)
                count += 1
            except Exception as e:
                logger.error(
                    f"Error emitting packet: {e}\n{traceback.format_exc()}"
                )

        time.sleep(0.001)


class UDPSingleton:
    """
    Legacy singleton facade to maintain backward compatibility.
    Delegates to the new specialized classes.
    """
    _thread_manager = ThreadManager()
    _address_registry = AddressRegistry()
    _message_id_generator = MessageIdGenerator()
    _packet_processor = PacketProcessor(_thread_manager)
    
    @classmethod
    def start_threads(cls):
        cls._thread_manager.start_threads(
            cls._packet_processor.incoming_thread_fn,
            cls._packet_processor.outgoing_thread_fn
        )
        cls._outgoing_message_id_counter_lock = threading.Lock()

    @classmethod
    def stop_threads(cls):
        cls._thread_manager.stop_threads()

    @classmethod
    def set_cluster_instance_addresses(cls, addresses: List[Tuple[int, str, int]]):
        cls._address_registry.set_addresses(addresses)
        cls._cluster_instance_addressses = addresses  # For backward compatibility

    @classmethod
    def get_cluster_instance_address(cls, instance_id: int) -> Tuple[str, int]:
        return cls._address_registry.get_address(instance_id)

    @classmethod
    def get_cluster_instance_addresses(cls) -> List[Tuple[int, str, int]]:
        return cls._address_registry.get_all_addresses()

    @classmethod
    def iterate_message_id(cls):
        return cls._message_id_generator.next_id()

    @classmethod
    def add_handle_incoming_packet_callback(cls, incoming_callback):
        cls._thread_manager.add_incoming_callback(incoming_callback)

    @classmethod
    def add_outgoing_thread_callback(cls, outgoing_callback):
        cls._thread_manager.add_outgoing_callback(outgoing_callback)

    @classmethod
    def _incoming_thread_fn(cls):
        cls._packet_processor.incoming_thread_fn()

    @classmethod
    def _outgoing_thread_fn(cls):
        cls._packet_processor.outgoing_thread_fn()

    @classmethod
    def process_batch_outgoing(cls, dequeue_outgoing_packet_fn, emit_fn):
        cls._packet_processor.process_batch_outgoing(dequeue_outgoing_packet_fn, emit_fn)


class ACKResult:
    """
    Represents the result of an acknowledgement operation.
    """
    def __init__(
        self,
        success: bool,
        data: object | None = None,
        error_msg: str | None = None,
    ):
        self.success = success
        self.data = data
        self.error_msg = error_msg


class IQueueManager(ABC):
    """
    Interface for managing a queue of outgoing packets.
    """
    @abstractmethod
    def has_packets_queued(self) -> bool:
        """Check if there are packets queued for sending."""
        pass
    
    @abstractmethod
    def queue_packet(self, packet: OutgoingPacket) -> None:
        """Add a packet to the outgoing queue."""
        pass
    
    @abstractmethod
    def dequeue_packet(self) -> Optional[OutgoingPacket]:
        """Remove and return a packet from the outgoing queue."""
        pass
        
    @abstractmethod
    def get_queue_size(self) -> int:
        """Get the current size of the outgoing queue."""
        pass


class IMessageHandler(ABC):
    """
    Interface for handling incoming and outgoing messages.
    """
    @abstractmethod
    def handle_incoming_packet(self, packet, sender_addr: str) -> None:
        """Handle an incoming packet."""
        pass
    
    @abstractmethod
    def process_outgoing_packets(self) -> None:
        """Process outgoing packets."""
        pass


class OutgoingQueueManager(IQueueManager):
    """
    Manages a queue of outgoing packets.
    """
    def __init__(self):
        self._outgoing_queue: queue.Queue = queue.Queue()
        self._outgoing_counter = 0
    
    def has_packets_queued(self) -> bool:
        """Check if there are packets queued for sending."""
        return self._outgoing_counter > 0
    
    def queue_packet(self, packet: OutgoingPacket) -> None:
        """Add a packet to the outgoing queue."""
        self._outgoing_queue.put_nowait(packet)
        self._outgoing_counter += 1
    
    def dequeue_packet(self) -> Optional[OutgoingPacket]:
        """Remove and return a packet from the outgoing queue."""
        if self._outgoing_counter == 0:
            return None
        try:
            outgoing_packet = self._outgoing_queue.get_nowait()
            self._outgoing_counter -= 1
            self._outgoing_queue.task_done()
        except queue.Empty:
            outgoing_packet = None
        return outgoing_packet
        
    def get_queue_size(self) -> int:
        """Get the current size of the outgoing queue."""
        return self._outgoing_queue.qsize()


class UDPBase(IMessageHandler):
    """
    Base class for UDP handlers with common functionality for sending and receiving messages.
    """
    def __init__(self, incoming_processed_packet_queue: queue.Queue):
        self._queue_manager = OutgoingQueueManager()
        self._incoming_processed_packet_queue = incoming_processed_packet_queue
        self._emitter = UDPEmitter(EnvVars.get_broadcast_port())

    def outgoing_packets_queued(self) -> bool:
        """Check if there are packets queued for sending."""
        return self._queue_manager.has_packets_queued()

    def queue_outgoing_packet(self, packet: OutgoingPacket) -> None:
        """Add a packet to the outgoing queue."""
        self._queue_manager.queue_packet(packet)

    def dequeue_outgoing_packet(self) -> Optional[OutgoingPacket]:
        """Remove and return a packet from the outgoing queue."""
        return self._queue_manager.dequeue_packet()
        
    def get_outgoing_queue_size(self) -> int:
        """Get the current size of the outgoing queue."""
        return self._queue_manager.get_queue_size()

    @abstractmethod
    def _handle_incoming_packet(self, packet, sender_addr: str) -> None:
        """Abstract method for handling received messages."""
        pass

    @abstractmethod
    def _outgoing_thread_callback(self) -> None:
        """Abstract method for handling message sending."""
        pass
    
    # Implement IMessageHandler interface
    def handle_incoming_packet(self, packet, sender_addr: str) -> None:
        """Handle an incoming packet."""
        self._handle_incoming_packet(packet, sender_addr)
    
    def process_outgoing_packets(self) -> None:
        """Process outgoing packets."""
        self._outgoing_thread_callback()
