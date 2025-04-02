import queue
import threading
import time
import traceback
from abc import ABC, abstractmethod

from ..log import logger
from ..env_vars import EnvVars
from .sender import UDPEmitter
from .listener import UDPListener
from .queued import IncomingPacket, IncomingBuffer, OutgoingPacket


class UDPSingleton:
    _broadcast_listener: UDPListener = None

    _incoming_thread = None
    _receive_async_loop = None
    _outgoing_thread = None
    _running: bool = False

    _incoming_thread_callbacks = []
    _outgoing_thread_callbacks = []
    _cluster_instance_addresses: list[tuple[int, str, int]] = []
    _message_index: int = 0
    # _incoming_queue: queue.Queue = queue.Queue()

    @classmethod
    def start_threads(cls):
        if not cls._running:
            cls._running = True
            cls._incoming_thread = threading.Thread(
                target=cls._incoming_thread_fn, daemon=True
            )
            cls._incoming_thread.name = "ComfyCluster-IncomingPackets"

            cls._outgoing_thread = threading.Thread(
                target=cls._outgoing_thread_fn, daemon=True
            )
            cls._outgoing_thread.name = "ComfyCluster-OutgoingPackets"

            cls._incoming_thread.start()
            cls._outgoing_thread.start()
            cls._outgoing_message_id_counter_lock = threading.Lock()

    @classmethod
    def stop_threads(cls):
        cls._running = False
        if cls._incoming_thread:
            cls._incoming_thread.join()
        if cls._outgoing_thread:
            cls._outgoing_thread.join()

    @classmethod
    def set_cluster_instance_addresses(
        cls, addresses: list[tuple[int, str, int]]
    ):
        logger.info("Setting cluster addresses: %s", addresses)
        cls._cluster_instance_addressses = addresses

    @classmethod
    def get_cluster_instance_address(
        cls, instance_id: int
    ) -> tuple[int, str, int]:
        return (
            cls._cluster_instance_addressses[instance_id][1],
            cls._cluster_instance_addressses[instance_id][2],
        )

    @classmethod
    def get_cluster_instance_addresses(cls) -> list[tuple[int, str, int]]:
        return cls._cluster_instance_addressses

    @classmethod
    def iterate_message_id(cls):
        with cls._outgoing_message_id_counter_lock:
            cls._message_index += 1
            return cls._message_index

    @classmethod
    def add_handle_incoming_packet_callback(cls, incoming_callback):
        cls._incoming_thread_callbacks.append(incoming_callback)

    @classmethod
    def add_outgoing_thread_callback(cls, outgoing_callback):
        cls._outgoing_thread_callbacks.append(outgoing_callback)

    # @classmethod
    # def _dequeue_packet(cls):
    #     incoming_packet = cls._incoming_queue.get()
    #     return

    @classmethod
    def _incoming_thread_fn(cls):
        logger.info("Starting incoming thread.")
        cls._broadcast_listener = UDPListener(
            EnvVars.get_listen_address(), EnvVars.get_broadcast_port()
        )
        cls._direct_listener = UDPListener(
            EnvVars.get_listen_address(), EnvVars.get_direct_listen_port()
        )
        step_packet_count = 0
        packet_count = 0

        while cls._running:
            packet, sender_addr = cls._broadcast_listener.poll()
            if packet is None or sender_addr is None:
                packet, sender_addr = cls._direct_listener.poll()
                if packet is None or sender_addr is None:
                    time.sleep(0.001)
                    continue

            if IncomingPacket.is_buffer(packet):
                incoming = IncomingBuffer(packet, sender_addr)
            else:
                incoming = IncomingPacket(packet, sender_addr)

            for callback in cls._incoming_thread_callbacks:
                try:
                    callback(incoming)
                except Exception as e:
                    logger.error(
                        f"Error in receive callback: {e}\n{traceback.format_exc()}"
                    )
            # cls._incoming_queue.put(IncomingPacket(packet, sender_addr))

            packet_count += 1
            if packet_count >= step_packet_count + 1000:
                # logger.info(f"Processed {packet_count} incoming packets")
                step_packet_count = packet_count

        logger.info("Exited incoming thread.")

    @classmethod
    def _outgoing_thread_fn(cls):
        while cls._running:
            for callback in cls._outgoing_thread_callbacks:
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

    @classmethod
    def process_batch_outgoing(cls, dequeue_outgoing_packet_fn, emit_fn):
        # Buffer size is 262144, MTU is typically 1500 bytes
        # So we can batch around 174 packets at a time
        batch_size = 174
        count = 0

        while count < batch_size:
            # Fast batch collection
            packet = dequeue_outgoing_packet_fn()
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


class ACKResult:
    def __init__(
        self,
        success: bool,
        data: object | None = None,
        error_msg: str | None = None,
    ):
        self.success = success
        self.data = data
        self.error_msg = error_msg


class UDPBase(ABC):
    def __init__(self, incoming_processed_packet_queue: queue.Queue):
        self._outgoing_queue: queue.Queue = queue.Queue()
        self._outgoing_counter = 0

        self._incoming_processed_packet_queue = incoming_processed_packet_queue
        self._emitter = UDPEmitter(EnvVars.get_broadcast_port())

    def outgoing_packets_queued(self):
        return self._outgoing_counter > 0

    def queue_outgoing_packet(self, packet: OutgoingPacket):
        self._outgoing_queue.put_nowait(packet)
        self._outgoing_counter += 1

    def dequeue_outgoing_packet(self) -> OutgoingPacket:
        if self._outgoing_counter == 0:
            return None
        try:
            outgoing_packet = self._outgoing_queue.get_nowait()
            self._outgoing_counter -= 1
            self._outgoing_queue.task_done()
        except queue.Empty:
            outgoing_packet = None
        return outgoing_packet

    @abstractmethod
    def _handle_incoming_packet(self, packet, sender_addr: str):
        """Abstract method for handling received messages"""

    @abstractmethod
    def _outgoing_thread_callback(self):
        """Abstract method for handling message sending"""
