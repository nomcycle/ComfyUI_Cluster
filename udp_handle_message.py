import asyncio
import queue
import time
import traceback
import os
import json
import socket
from typing import Dict
import threading

from google.protobuf.json_format import ParseDict
from .protobuf.messages_pb2 import (ClusterMessageType, ClusterAck)

from .log import logger
from .env_vars import EnvVars
from .udp_base import UDPBase
from .udp_base import UDPSingleton
from .queued import IncomingPacket, IncomingMessage, OutgoingPacket
from .pending_messages import PendingMessage, PendingInstanceMessage
    
class ACKResult:
    def __init__(self, success: bool, error_msg: str = None):
        self.success = success
        self.error_msg = error_msg

class UDPMessageHandler(UDPBase):
    def __init__(self, state_loop, incoming_processed_packet_queue: queue.Queue):
        super().__init__(incoming_processed_packet_queue)
        logger.info("Initializing UDP handler")
        self._instance_id = EnvVars.get_instance_index()
        self._pending_acks: Dict[int, PendingMessage] = {}
        self._state_loop = state_loop
        self._local_ips: [str] = None

        self._outgoing_thread_lock = threading.Lock()

        UDPSingleton.add_outgoing_thread_callback(self._outgoing_thread_callback)
        UDPSingleton.add_handle_incoming_packet_callback(self._handle_incoming_packet)

    def _handle_incoming_packet(self, incoming_packet: IncomingPacket):
        try:
            if incoming_packet.get_is_buffer():
                return
            incoming_msg: IncomingMessage = self._validate_incoming_message(incoming_packet)
            if not incoming_msg:
                return

            # logger.debug("(Received) UDP message from %s:%d:\n%s", incoming_msg.sender_addr, EnvVars.get_listen_port(), json.dumps(incoming_msg.message, indent=2))
            logger.debug(str(incoming_msg))
            self._process_incoming_message(incoming_msg)

        except Exception as e:
            logger.error("Receive loop error: %s\n%s", e, traceback.format_exc())
    
    def _outgoing_thread_callback(self):
        try:
            self._process_pending_messages()
            UDPSingleton.process_batch_outgoing(
                self.dequeue_outgoing_packet,
                lambda msg: self._send_message(msg))

        except Exception as e:
            logger.error("Send loop error: %s\n%s", e, traceback.format_exc())

    def cancel_all_pending(self):
        logger.info("Cancelling all pending messages")
        for message_id, pending in list(self._pending_acks.items()):
            if not pending.future.done():
                self._complete_future(pending.future, False, "Cancelled")
            del self._pending_acks[message_id]

    def _process_pending_messages(self):
        current_time = time.time()
        with self._outgoing_thread_lock:
            for message_id, pending in list(self._pending_acks.items()):
                for instance_id, pending_ack in list(pending.pending_acks.items()):
                    if pending.should_retry(instance_id, current_time):
                        if pending.has_exceeded_retries(instance_id):
                            logger.warning("Max retries exceeded - msg %s to %s", message_id, instance_id)
                            del pending.pending_acks[instance_id]
                            
                            if len(pending.pending_acks) == 0:
                                del self._pending_acks[message_id]
                                if not pending.future.done():
                                    self._complete_future(pending.future, False, "Max retries exceeded")
                        else:
                            retry_count = pending.increment_retry(instance_id)
                            logger.info("Reattempting to send msg: %s to instance: %s (%s/%s)", message_id, instance_id, retry_count, pending.MAX_RETRIES)
                            self._queue_outgoing_to_instance(pending.message, pending_ack.instance_id)

    def _send_message(self, queued_msg):
        if queued_msg.optional_addr is not None:
            self._emit_message(queued_msg.packet, queued_msg.optional_addr)
        elif EnvVars.get_udp_broadcast():
            self._emit_message(queued_msg.packet)
        else: # Loop through each hostname and emit message directly.
            for instance_id, instance_addr in UDPSingleton.get_cluster_instance_addresses():
                if instance_id == EnvVars.get_instance_index():
                    continue
                self._emit_message(queued_msg.packet, instance_addr)

    def _handle_outgoing_failure(self, message_id: int):
        for pending_key, pending in list(self._pending_acks.items()):
            if str(pending.message_id) == str(message_id):
                if not pending.future.done():
                    self._complete_future(pending.future, False, "Send failed")

    def _complete_future(self, future, success: bool, error_msg: str = None):
        if not success:
            logger.error("Future failed: %s", error_msg)
        self._state_loop.call_soon_threadsafe(future.set_result, ACKResult(success, error_msg))
    
    def _validate_incoming_message(self, packet: IncomingPacket):
        if packet.get_is_buffer():
            return None
        message = json.loads(packet.packet.decode())
        header = message.get('header', None)

        if not header:
            raise ValueError("Missing message header")

        if not EnvVars.get_single_host() and packet.sender_addr in self.get_cached_local_addreses():
            return None

        sender_instance_id = header.get('senderInstanceId', -1) - 1
        if sender_instance_id is None or sender_instance_id == '':
            logger.error("Empty sender ID")
            return None
        if sender_instance_id == self._instance_id:
            return None
        msg_type_str = header.get('type', '')
        if msg_type_str == '':
            logger.error("Unknown message type")
            return None
        msg_type = ClusterMessageType.Value(msg_type_str)
        if msg_type < 0:
            logger.error("Unknown message type")
            return None
        message_id = header.get('messageId', -1)
        if message_id == -1:
            logger.error("Missing message ID")
            return None

        require_ack: bool = header.get('requireAck', False)
        return IncomingMessage(
            packet.sender_addr,
            sender_instance_id,
            msg_type_str,
            message_id,
            msg_type,
            require_ack,
            message)

    def _process_incoming_message(self, incoming_msg: IncomingMessage):
        if incoming_msg.msg_type == ClusterMessageType.ACK:
            self._handle_ack(incoming_msg)
            return
        self._handle_non_ack_message(incoming_msg)

    def _handle_non_ack_message(self, incoming_msg: IncomingMessage):
        if incoming_msg.require_ack:
            self._send_ack(incoming_msg.message_id, incoming_msg.sender_addr)
        try:
            # Use put_nowait to avoid blocking indefinitely
            self._incoming_processed_packet_queue.put_nowait(incoming_msg)
            
            queue_size = self._incoming_processed_packet_queue.qsize()
            if queue_size % 100 == 0:
                logger.debug('Incoming message queue size: %s', queue_size)
                
        except queue.Full:
            logger.warning(f"Message queue full, dropping message {incoming_msg.message_id}")

    def _emit_message(self, msg, addr: str | None = None):
        self._emitter.emit_message(msg, addr)

    def _send_ack(self, message_id: int, addr: str):
        logger.debug("Sending ACK for message %d to %s", message_id, addr)
        ack = ClusterAck()
        ack.header.type = ClusterMessageType.ACK
        ack.header.message_id = UDPSingleton.iterate_message_id()
        ack.header.sender_instance_id = self._instance_id + 1
        ack.ack_message_id = message_id
        self._queue_outgoing_to_addr(ack, addr)

    def _handle_ack(self, incoming_msg: IncomingMessage):
        ack = ParseDict(incoming_msg.message, ClusterAck())
        if ack.ack_message_id in self._pending_acks:
            logger.debug("Received ACK message from %s: for message: %s", incoming_msg.sender_addr, ack.ack_message_id)
            self._process_ack(ack.ack_message_id, incoming_msg)
        else:
            logger.warning("ACK for unknown msg %s", ack.ack_message_id)

    def _process_ack(self, ack_message_id: int, incoming_msg: IncomingMessage):
        pending_msg = self._pending_acks[ack_message_id]
        if incoming_msg.sender_instance_id in pending_msg.pending_acks:
            logger.debug("Removing pending ACK for instance %i from message %d", incoming_msg.sender_instance_id, ack_message_id)
            del pending_msg.pending_acks[incoming_msg.sender_instance_id]
            if len(pending_msg.pending_acks) == 0 and not pending_msg.future.done():
                logger.debug("All ACKs received for message %d, completing future", ack_message_id)
                del self._pending_acks[ack_message_id]
                self._complete_future(pending_msg.future, True, None)
        else:
            logger.warning("Duplicate ACK from %s for msg %s", incoming_msg.sender_instance_id, ack_message_id)

    def _prepare_message(self, message):
        message_id = UDPSingleton.iterate_message_id()
        message.header.message_id = message_id
        message.header.sender_instance_id = self._instance_id + 1
        return message_id

    def _create_pending_message(self, message_id: int, message, instance_id: int):
        pending_msg = PendingMessage(message_id, message)
        pending_msg.future = self._state_loop.create_future()

        if instance_id is not None:
            pending_msg.pending_acks[instance_id] = PendingInstanceMessage(time.time(), 0, instance_id)
        for instance_id, _ in UDPSingleton.get_cluster_instance_addresses():
            if instance_id == self._instance_id:
                continue
            pending_msg.pending_acks[instance_id] = PendingInstanceMessage(time.time(), 0, instance_id)
        self._pending_acks[message_id] = pending_msg

        return pending_msg

    def send_no_wait(self, message, instance_id: int | None = None):
        message_id = None
        # with self._outgoing_thread_lock:
        message_id = self._prepare_message(message)
        self._queue_outgoing_to_instance(message, instance_id)
        return message_id

    async def send_and_wait(self, message, instance_id: int | None = None):
        if not message.header.require_ack:
            _ = self.send_no_wait(message, instance_id)
            await asyncio.sleep(0)
            return ACKResult(True, None)

        # with self._outgoing_thread_lock:
        message_id = self._prepare_message(message)
        pending_msg = self._create_pending_message(message_id, message, instance_id)

        self._queue_outgoing_to_instance(message, instance_id)

        result = await pending_msg.future
        if not result.success:
            logger.error("No ACK for msg %s: %s", message_id, result.error_msg)

        return result

    async def send_and_wait_thread_safe(self, message, instance_id: int | None = None):
        current_loop = asyncio.get_running_loop()
        future = current_loop.create_future()
        
        def done_callback(task):
            try:
                result = task.result()
                current_loop.call_soon_threadsafe(future.set_result, result)
            except Exception as e:
                current_loop.call_soon_threadsafe(future.set_exception, e)
        
        coroutine = self.send_and_wait(message, instance_id)

        try:
            asyncio.run_coroutine_threadsafe(
                coroutine,
                self._state_loop
            ).add_done_callback(done_callback)
        except Exception as e:
            logger.exception('Encountered exception while attempting to execute coroutine on another asyncio loop:\n%s', e)
        
        return await future

    def _log_outgoing_queue_size(self):
        outgoing_queue_size = self._outgoing_queue.qsize()
        # if outgoing_queue_size % 100 == 0:
        logger.info('Outgoing message queue size: %s', outgoing_queue_size)

    def _queue_outgoing(self, packet):
        queued_msg = OutgoingPacket(packet, None)
        self.queue_outgoing_packet(queued_msg)
        self._log_outgoing_queue_size()

    def _queue_outgoing_to_instance(self, packet, instance_id: int | None):
        if instance_id is None:
            self._queue_outgoing(packet)
            return
        addr = UDPSingleton.get_cluster_instance_address(instance_id)
        queued_msg = OutgoingPacket(packet, addr)
        self.queue_outgoing_packet(queued_msg)
        self._log_outgoing_queue_size()

    def _queue_outgoing_to_addr(self, packet, addr: str):
        queued_msg = OutgoingPacket(packet, addr)
        self.queue_outgoing_packet(queued_msg)
        self._log_outgoing_queue_size()

    def get_cached_local_addreses(self):
        if self._local_ips is None:
            interfaces = socket.getaddrinfo(socket.gethostname(), None)
            self._local_ips = [interface[4][0] for interface in interfaces]
            logger.debug("Local IP addresses: %s", self._local_ips)
        return self._local_ips