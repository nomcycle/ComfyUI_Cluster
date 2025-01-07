import asyncio
import queue
import threading
import time
import traceback
import os
from typing import Dict

from google.protobuf.json_format import ParseDict, MessageToJson
from .protobuf.messages_pb2 import (ClusterMessageType, ClusterAck)

from .sender import UDPSender
from .listener import UDPListener
from .log import logger
from .env_vars import EnvVars

class IncomingQueuedMessage:
    def __init__(self, header, message, optional_addr: str = None):
        self.header = header
        self.message = message
        self.optional_addr: str = optional_addr

class OutgoingQueuedMessage:
    def __init__(self, message, optional_addr: str = None):
        self.message = message
        self.optional_addr: str = optional_addr

class IncomingQueuedBuffer:
    def __init__(self, byte_buffer: bytes, optional_addr: str = None):
        self.byte_buffer: bytes = byte_buffer
        self.optional_addr: str = optional_addr

class OutgoingQueuedBuffer:
    def __init__(self, byte_buffer: bytes, optional_addr: str = None):
        self.byte_buffer: bytes = byte_buffer
        self.optional_addr: str = optional_addr

class PendingInstanceMessage:
    def __init__(self, timestamp: float, retry_count: int, addr: str):
        self.timestamp: float = timestamp
        self.retry_count: int = retry_count
        self.addr: str = addr

class PendingMessage:
    def __init__(self, message_id: int, message):
        self.message_id: int = message_id
        self.message = message
        self.pending_acks: Dict[str, PendingInstanceMessage] = {} # Dict of addr -> {timestamp: float, retry_count: int}
        self.future = None
        self.MAX_RETRIES: int = 10

    def increment_retry(self, addr: str):
        if addr not in self.pending_acks:
            self.pending_acks[addr] = PendingInstanceMessage(time.time(), 0, addr)
        self.pending_acks[addr].retry_count += 1
        self.pending_acks[addr].timestamp = time.time()
        return self.pending_acks[addr].retry_count

    def has_exceeded_retries(self, addr: str):
        if addr not in self.pending_acks:
            return False
        return self.pending_acks[addr].retry_count >= self.MAX_RETRIES
        
    def should_retry(self, addr: str, current_time: float):
        if addr not in self.pending_acks:
            return False
        last_try = self.pending_acks[addr].timestamp
        return current_time - last_try > 3.0 
    
class ACKResult:
    def __init__(self, success: bool, error_msg: str = None):
        self.success = success
        self.error_msg = error_msg

class UDP:
    def __init__(self, message_callback, buffer_callback, async_loop, instance_id: str):
        logger.info("Initializing UDP handler")
        self._init_components(message_callback, buffer_callback, async_loop, instance_id)
        self._start_threads()

    def _init_components(self, message_callback, buffer_callback, async_loop, instance_id):
        self._instance_id: str = instance_id

        self._message_callback = message_callback
        self._buffer_callback = buffer_callback

        # Outgoing queues
        self._outgoing_message_queue: queue.Queue = queue.Queue()
        self._outgoing_byte_buffer_queue: queue.Queue = queue.Queue()

        # Incoming queues 
        self._incoming_message_queue: queue.Queue = queue.Queue()
        self._incoming_byte_buffer_queue: queue.Queue = queue.Queue()

        self._message_index: int = 0
        self._pending_acks: Dict[int, PendingMessage] = {}
        self._cluster_instance_addressses: [str] = []
        self._loop = async_loop
        self._running: bool = True

        self._listener = UDPListener(EnvVars.get_listen_address(), EnvVars.get_listen_port(), self._handle_message, self._handle_buffer)
        self._sender = UDPSender(EnvVars.get_send_port())

    def _start_threads(self):
        self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._send_thread = threading.Thread(target=self._send_and_retry_loop, daemon=True)
        self._receive_thread.start()
        self._send_thread.start()

    def _iterate_message_id(self):
        self._message_index += 1
        return self._message_index

    def set_cluster_instance_addresses(self, addresses: [str]):
        logger.info("Setting cluster addresses: %s", addresses)
        self._cluster_instance_addressses = addresses

    def _receive_loop(self):
        logger.info("Starting receive loop")
        while self._running:
            try:
                self._listener.poll()
                while not self._incoming_message_queue.empty():
                    msg = self._incoming_message_queue.get()
                    self._process_message(msg.header, msg.message, msg.optional_addr)
                    self._incoming_message_queue.task_done()
                while not self._incoming_byte_buffer_queue.empty():
                    buf = self._incoming_byte_buffer_queue.get()
                    self._process_buffer(buf.byte_buffer, buf.optional_addr)
                    self._incoming_byte_buffer_queue.task_done()
            except Exception as e:
                logger.error("Receive loop error: %s\n%s", e, traceback.format_exc())
            time.sleep(0.01)
        logger.info("Exited receive loop.")
    
    def _send_and_retry_loop(self):
        logger.info("Starting send/retry loop")
        while self._running:
            try:
                self._process_pending_messages()
                while not self._outgoing_message_queue.empty():
                    queued_msg = self._outgoing_message_queue.get()
                    self._send_message(queued_msg)
                    self._outgoing_message_queue.task_done()
                while not self._outgoing_byte_buffer_queue.empty():
                    queued_buffer = self._outgoing_byte_buffer_queue.get()
                    self._emit_byte_buffer(queued_buffer.byte_buffer, queued_buffer.optional_addr)
                    self._outgoing_byte_buffer_queue.task_done()
            except Exception as e:
                logger.error("Send loop error: %s\n%s", e, traceback.format_exc())
            time.sleep(0.1)
        logger.info("Exited send loop.")

    def cancel_all_pending(self):
        logger.info("Cancelling all pending messages")
        for message_id, pending in list(self._pending_acks.items()):
            if not pending.future.done():
                self._complete_future(pending.future, False, "Cancelled")
            del self._pending_acks[message_id]

    def _process_pending_messages(self):
        current_time = time.time()
        for message_id, pending in list(self._pending_acks.items()):
            for addr, pending_ack in list(pending.pending_acks.items()):
                if pending.should_retry(addr, current_time):
                    # if pending.has_exceeded_retries(addr):
                    #     self._handle_max_retries_exceeded(message_id, pending, addr)
                    # else:
                    #     self._retry_message(message_id, pending, addr)
                    self._retry_message(message_id, pending, addr)

    def _handle_max_retries_exceeded(self, message_id: int, pending, addr: str):
        logger.warning("Max retries exceeded - msg %s to %s", message_id, addr)
        del pending.pending_acks[addr]
        
        if len(pending.pending_acks) == 0:
            del self._pending_acks[message_id]
            if not pending.future.done():
                self._complete_future(pending.future, False, "Max retries exceeded")

    def _retry_message(self, message_id: int, pending, addr: str):
        retry_count = pending.increment_retry(addr)
        logger.info("Retry %s/%s - msg %s to %s", retry_count, pending.MAX_RETRIES, message_id, addr)
        self._outgoing_message_queue.put(OutgoingQueuedMessage(pending.message, addr))

    def _process_message_queue(self):
        while not self._outgoing_message_queue.empty():
            try:
                queued_msg = self._outgoing_message_queue.get()
                self._send_message(queued_msg)
                self._outgoing_message_queue.task_done()
                
            except Exception as e:
                msg_id = queued_msg.message.header.message_id
                logger.error("Error processing message from queue (msg_id=%s): %s\n%s", msg_id, e, traceback.format_exc())
                self._handle_send_failure(msg_id)
                raise e

    def _send_message(self, queued_msg):
        if queued_msg.optional_addr is not None:
            self._emit_message(queued_msg.message, queued_msg.optional_addr)
        elif EnvVars.get_udp_broadcast():
            self._emit_message(queued_msg.message)
        else:
            self._send_to_all_instances(queued_msg.message)

    def _send_to_all_instances(self, message):
        for hostname in self._cluster_instance_addressses:
            self._emit_message(message, hostname)

    def _handle_send_failure(self, message_id: int):
        for pending_key, pending in list(self._pending_acks.items()):
            if str(pending.message_id) == str(message_id):
                if not pending.future.done():
                    self._complete_future(pending.future, False, "Send failed")

    def _complete_future(self, future, success: bool, error_msg: str = None):
        if not success:
            logger.error("Future failed: %s", error_msg)
        self._loop.call_soon_threadsafe(future.set_result, ACKResult(success, error_msg))

    def _handle_buffer(self, buffer, addr: str):
        self._incoming_byte_buffer_queue.put(IncomingQueuedBuffer(buffer, addr))

    def _process_buffer(self, byte_buffer: bytes, addr: str):
        self._buffer_callback(byte_buffer, addr)

    def _handle_message(self, header, message, addr: str):
        self._incoming_message_queue.put(IncomingQueuedMessage(header, message, addr))

    def _process_message(self, header, message, addr: str):
        sender_instance_id = header.get('senderInstanceId', "")
        if sender_instance_id is None or sender_instance_id == '':
            logger.error("Empty sender ID")
            return

        if sender_instance_id == self._instance_id:
            return

        msg_type_str = header.get('type', -1)
        msg_type = ClusterMessageType.Value(msg_type_str)
        if msg_type == -1:
            logger.error("Unknown message type")
            return

        message_id = header.get('messageId', -1)
        if message_id == -1:
            logger.error("Missing message ID")
            return

        if msg_type == ClusterMessageType.ACK:
            self._handle_ack(message, addr)
        else:
            require_ack: bool = header.get('requireAck', False)
            self._handle_non_ack_message(message, msg_type_str, message_id, addr, require_ack)

    def _handle_non_ack_message(self, message: str, msg_type: str, message_id: int, addr: str, send_ack: bool):
        if send_ack:
            self._send_ack(message_id, addr)
        self._message_callback(msg_type, message, addr)
    
    def queue_byte_buffer(self, byte_buffer, addr: str = None):
        self._outgoing_byte_buffer_queue.put(OutgoingQueuedBuffer(byte_buffer, addr))

    def _emit_message(self, msg, addr: str = None):
        msg.header.process_id = os.getpid()
        self._sender.send(msg, addr)

    def _emit_byte_buffer(self, byte_buffer: bytes, addr: str = None):
        self._sender.send_bytes(byte_buffer, addr)

    def _send_ack(self, message_id: int, addr: str):
        logger.debug("Sending ACK for message %d to %s", message_id, addr)
        ack = ClusterAck()
        ack.header.type = ClusterMessageType.ACK
        ack.header.message_id = self._iterate_message_id()
        ack.header.sender_instance_id = self._instance_id
        ack.ack_message_id = message_id
        self._emit_message(ack, addr)

    def _handle_ack(self, message, addr: str):
        ack = ParseDict(message, ClusterAck())
        if ack.ack_message_id in self._pending_acks:
            logger.debug("Received ACK message from %s: for message: %s", addr, ack.ack_message_id)
            self._process_ack(ack.ack_message_id, addr)
        else:
            logger.warning("ACK for unknown msg %s", ack.ack_message_id)

    def _process_ack(self, message_id: int, addr: str):
        pending_msg = self._pending_acks[message_id]
        if addr in pending_msg.pending_acks:
            logger.debug("Removing pending ACK for addr %s from message %d", addr, message_id)
            del pending_msg.pending_acks[addr]
            if len(pending_msg.pending_acks) == 0 and not pending_msg.future.done():
                logger.debug("All ACKs received for message %d, completing future", message_id)
                del self._pending_acks[message_id]
                self._complete_future(pending_msg.future, True, None)
        else:
            logger.warning("Duplicate ACK from %s for msg %s", addr, message_id)

    def send_no_wait(self, message, addr: str = None):
        message_id = self._prepare_message(message)
        self._queue_message(message, addr)
        return message_id

    async def send_and_wait(self, message, addr: str = None):
        if not message.header.require_ack:
            _ = self.send_no_wait(message, addr)
            await asyncio.sleep(0)
            return ACKResult(True, None)

        message_id = self.send_no_wait(message, addr)
        pending_msg = self._create_pending_message(message_id, message, addr)
        result = await pending_msg.future

        if not result.success:
            logger.error("No ACK for msg %s: %s", message_id, result.error_msg)

        return result

    async def send_and_wait_thread_safe(self, message, addr: str = None):
        current_loop = asyncio.get_running_loop()
        future = current_loop.create_future()
        
        def done_callback(task):
            try:
                result = task.result()
                current_loop.call_soon_threadsafe(future.set_result, result)
            except Exception as e:
                current_loop.call_soon_threadsafe(future.set_exception, e)
        
        asyncio.run_coroutine_threadsafe(
            self.send_and_wait(message, addr),
            self._loop
        ).add_done_callback(done_callback)
        
        return await future

    def _prepare_message(self, message):
        message_id = self._iterate_message_id()
        message.header.message_id = message_id
        message.header.sender_instance_id = self._instance_id
        return message_id

    def _queue_message(self, message, addr: str):
        queued_msg = OutgoingQueuedMessage(message, addr)
        self._outgoing_message_queue.put(queued_msg)

    def _create_pending_message(self, message_id: int, message, addr: str):
        pending_msg = PendingMessage(message_id, message)
        pending_msg.future = self._loop.create_future()

        if addr is not None:
            pending_msg.pending_acks[addr] = PendingInstanceMessage(time.time(), 0, addr)
        elif EnvVars.get_udp_broadcast():
            for instance_addr in self._cluster_instance_addressses:
                pending_msg.pending_acks[instance_addr] = PendingInstanceMessage(time.time(), 0, instance_addr)
        self._pending_acks[message_id] = pending_msg

        return pending_msg

    def __del__(self):
        logger.info("Shutting down UDP handler")
        self._running = False
        if hasattr(self, '_receive_thread'):
            self._receive_thread.join(timeout=1.0)
        if hasattr(self, '_send_thread'):
            self._send_thread.join(timeout=1.0)