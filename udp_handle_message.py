import asyncio
import queue
import time
import traceback
import os
import json
import socket
from typing import Dict
import threading
from enum import Enum, auto

from google.protobuf.json_format import ParseDict
from .protobuf.messages_pb2 import (ClusterMessageType, ClusterAck, ClusterRequestState, ClusterResolvedState, ClusterState, ClusterAwaitingFence)

from .log import logger
from .env_vars import EnvVars
from .udp_base import UDPBase, ACKResult
from .udp_base import UDPSingleton
from .queued import IncomingPacket, IncomingMessage, OutgoingPacket
from .pending_messages import PendingMessage, PendingInstanceMessage
    
class UDPMessageHandler(UDPBase):
    def __init__(self, state_loop, incoming_processed_packet_queue: queue.Queue):
        super().__init__(incoming_processed_packet_queue)
        logger.info("Initializing UDP handler")
        self._instance_id = EnvVars.get_instance_index()
        self._pending_acks: Dict[int, PendingMessage] = {}
        self._state_loop = state_loop
        self._local_ips: [str] = None

        self._awaiting_state_resolves: Dict[int, set[int]] = {}
        self._awaiting_state_resolve_futures: Dict[int, asyncio.Future] = {}

        self._instance_fence_states: Dict[int, Dict[int, bool]] = {}
        self._instance_fence_futures: Dict[int, asyncio.Future] = {}
        self._queued_instance_fence_signals: Dict[int, queue.Queue[IncomingMessage]] = {}

        self._received_expected_messages: Dict[int, IncomingMessage] = {}
        self._awaiting_expected_futures: Dict[int, asyncio.Future] = {}

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
            self._process_pending_fence_signals()
            self._process_received_expected_messages()

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

    def _process_outgoing_message_acks(self):
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

    def _process_pending_fence_signals(self):
        # Process any queued fence signals for active fence states
        for fence_id in list(self._instance_fence_states.keys()):
            queued_signals = self._queued_instance_fence_signals.get(fence_id)
            if queued_signals:
                while True:
                    try:
                        incoming_msg = queued_signals.get_nowait()
                        self._handle_await_Fence(incoming_msg)
                    except queue.Empty:
                        break

    def _process_received_expected_messages(self):
        # Process any received expected messages that have matching awaiting futures
        for expected_key in list(self._awaiting_expected_futures.keys()):
            if expected_key in self._received_expected_messages:
                self._complete_future(self._awaiting_expected_futures[expected_key], True, self._received_expected_messages[expected_key], None)
                del self._awaiting_expected_futures[expected_key]
                del self._received_expected_messages[expected_key]

    def _process_pending_messages(self):
        self._process_outgoing_message_acks()

    def _send_message(self, queued_msg: OutgoingPacket):
        if queued_msg.optional_addr is not None:
            self._emit_message(queued_msg.packet, queued_msg.optional_addr)
        elif EnvVars.get_udp_broadcast():
            self._emit_message(queued_msg.packet)
        else: # Loop through each hostname and emit message directly.
            for instance_id, instance_addr, direct_listening_port in UDPSingleton.get_cluster_instance_addresses():
                if instance_id == EnvVars.get_instance_index():
                    continue
                self._emit_message(queued_msg.packet, (instance_addr, direct_listening_port))

    def _handle_outgoing_failure(self, message_id: int):
        for pending_key, pending in list(self._pending_acks.items()):
            if str(pending.message_id) == str(message_id):
                if not pending.future.done():
                    self._complete_future(pending.future, False, "Send failed")

    def _complete_future(self, future, success: bool, data: object | None = None, error_msg: str = None):
        if not success:
            logger.error("Future failed: %s", error_msg)
        self._state_loop.call_soon_threadsafe(future.set_result, ACKResult(success, data, error_msg=error_msg))
    
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
        expected_key = header.get('expectedKey', -1)

        return IncomingMessage(
            packet.sender_addr,
            sender_instance_id,
            msg_type_str,
            message_id,
            msg_type,
            require_ack,
            expected_key,
            message)

    def _process_incoming_message(self, incoming_msg: IncomingMessage):
        if incoming_msg.msg_type == ClusterMessageType.ACK:
            self._handle_ack(incoming_msg)
            return
        else:
            if incoming_msg.msg_type == ClusterMessageType.RESOLVED_STATE:
                self._handle_resolved_state(incoming_msg)
            elif incoming_msg.msg_type == ClusterMessageType.AWAITING_FENCE:
                self._handle_await_Fence(incoming_msg)
            elif incoming_msg.expected_key > -1:
                self._handle_expected_message(incoming_msg)
            else: self._handle_non_ack_message(incoming_msg)

            if incoming_msg.require_ack:
                addr, port = UDPSingleton.get_cluster_instance_address(incoming_msg.sender_instance_id)
                self._send_ack(incoming_msg.message_id, (addr, port))

    def _handle_non_ack_message(self, incoming_msg: IncomingMessage):
        try:
            # Use put_nowait to avoid blocking indefinitely
            self._incoming_processed_packet_queue.put_nowait(incoming_msg)
            
            queue_size = self._incoming_processed_packet_queue.qsize()
            if queue_size % 100 == 0:
                logger.debug('Incoming message queue size: %s', queue_size)
                
        except queue.Full:
            logger.warning(f"Message queue full, dropping message {incoming_msg.message_id}")

    def _emit_message(self, msg, addr_and_port: tuple[str, int] | None = None):
        self._emitter.emit_message(msg, addr_and_port)

    def _send_ack(self, message_id: int, addr_and_port: tuple[str, int] | None = None):
        if addr_and_port is not None:
            logger.debug("Sending ACK for message %d to %s:%s", message_id, addr_and_port[0], addr_and_port[1])
        else:
            logger.debug("Broadcasting ACK for message %d", message_id)
        ack = ClusterAck()
        ack.header.type = ClusterMessageType.ACK
        ack.header.message_id = UDPSingleton.iterate_message_id()
        ack.header.sender_instance_id = self._instance_id + 1
        ack.ack_message_id = message_id
        self._queue_outgoing_to_addr(ack, addr_and_port)

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

    def _create_pending_message(self, message_id: int, message, instance_id: int | None = None):
        pending_msg = PendingMessage(message_id, message)
        pending_msg.future = self._state_loop.create_future()

        if instance_id is not None:
            pending_msg.pending_acks[instance_id] = PendingInstanceMessage(time.time(), 0, instance_id)
        else:
            for instance_id, _, _ in UDPSingleton.get_cluster_instance_addresses():
                if instance_id == self._instance_id:
                    continue
                pending_msg.pending_acks[instance_id] = PendingInstanceMessage(time.time(), 0, instance_id)
        self._pending_acks[message_id] = pending_msg

        return pending_msg

    def _handle_resolved_state(self, incoming_msg: IncomingMessage):
        resolve_state = ParseDict(incoming_msg.message, ClusterResolvedState())

        if resolve_state.request_message_id in self._awaiting_state_resolves:

            if incoming_msg.sender_instance_id in self._awaiting_state_resolves[resolve_state.request_message_id]:
                self._awaiting_state_resolves[resolve_state.request_message_id].remove(incoming_msg.sender_instance_id)
            else:
                logger.error("Duplicate resolve state from %s for request message ID: %s", incoming_msg.sender_instance_id, resolve_state.request_message_id)

            if len(self._awaiting_state_resolves[resolve_state.request_message_id]) == 0:
                del self._awaiting_state_resolves[resolve_state.request_message_id]
                self._complete_future(self._awaiting_state_resolve_futures[resolve_state.request_message_id], True, None)
        else:
            logger.error("No awaiting state resolves for request message ID: %s", resolve_state.request_message_id)

    def _create_request_state_msg(self, state: ClusterState):
        message = ClusterRequestState()
        message.header.type = ClusterMessageType.REQUEST_STATE
        message.header.require_ack = True
        message.state = state
        return message

    def _prepare_request_state_msg(self, message: ClusterRequestState, future: asyncio.Future) -> tuple[int, PendingMessage]:
        message_id = self._prepare_message(message)
        pending_msg = self._create_pending_message(message_id, message)

        instance_ids = set(range(EnvVars.get_instance_count()))
        instance_ids.remove(EnvVars.get_instance_index())
        self._awaiting_state_resolves[message_id] = instance_ids

        self._awaiting_state_resolve_futures[message_id] = future
        return message_id, pending_msg

    async def _execute_coroutine_thread_safe(self, coroutine):
        current_loop = asyncio.get_running_loop()
        future = current_loop.create_future()

        def done_callback(task):
            try:
                result = task.result()
                current_loop.call_soon_threadsafe(future.set_result, result)
            except Exception as e:
                current_loop.call_soon_threadsafe(future.set_exception, e)

        try:
            asyncio.run_coroutine_threadsafe(
                coroutine,
                self._state_loop
            ).add_done_callback(done_callback)
        except Exception as e:
            logger.exception('Encountered exception while attempting to execute coroutine on another asyncio loop:\n%s', e)

        return await future

    def _handle_expected_message(self, incoming_message: IncomingMessage):
        expected_key = incoming_message.expected_key
        if expected_key in self._awaiting_expected_futures:
            self._complete_future(self._awaiting_expected_futures[expected_key], True, incoming_message)
            del self._awaiting_expected_futures[expected_key]
        else:
            if expected_key in self._received_expected_messages:
                raise Exception(f"Received duplicate expected message with key {expected_key}")
            self._received_expected_messages[expected_key] = incoming_message

    async def await_expected_message_thread_safe(self, expected_key: int):
        return await self._execute_coroutine_thread_safe(self.await_exepected_message(expected_key))

    async def send_expected_message_thread_safe(self, message, expected_key: int, instance_id: int | None = None):
        return await self._execute_coroutine_thread_safe(self.send_exepected_message(message, expected_key, instance_id))
    
    async def await_exepected_message(self, expected_key: int):
        if expected_key in self._awaiting_expected_futures:
            raise Exception(f"Already awaiting message with key {expected_key}")

        future = self._state_loop.create_future()
        self._awaiting_expected_futures[expected_key] = future
        return await future

    async def send_exepected_message(self, message, expected_key: int, instance_id: int | None = None):
        message.header.expected_key = expected_key
        return await self.send_and_wait(message, instance_id)

    def _handle_await_Fence(self, incoming_msg: IncomingMessage):
        await_fence = ParseDict(incoming_msg.message, ClusterAwaitingFence())
        if await_fence.fence_id in self._instance_fence_states:
            if incoming_msg.sender_instance_id in self._instance_fence_states[await_fence.fence_id]:
                self._instance_fence_states[await_fence.fence_id][incoming_msg.sender_instance_id] = True
                if all(self._instance_fence_states[await_fence.fence_id].values()):
                    self._complete_future(self._instance_fence_futures[await_fence.fence_id], True, None)
                    del self._instance_fence_states[await_fence.fence_id]
            else:
                logger.debug(f"Instance {incoming_msg.sender_instance_id} not found in fence states for fence {await_fence.fence_id}")
        else:
            if await_fence.fence_id not in self._queued_instance_fence_signals:
                self._queued_instance_fence_signals[await_fence.fence_id] = queue.Queue()
            self._queued_instance_fence_signals[await_fence.fence_id].put_nowait(incoming_msg)
            # logger.debug(f"No fence states found for fence {await_fence.fence_id}")

    async def await_fence_thread_safe(self, fence_id: int):
        return await self._execute_coroutine_thread_safe(self.await_fence(fence_id))

    async def await_fence(self, fence_id: int):
        if fence_id in self._instance_fence_states:
            msg = f"Previous fence with id {fence_id} has not resolved"
            logger.error(msg)
            return ACKResult(False, error_msg=msg)

        future = self._state_loop.create_future()
        message = ClusterAwaitingFence()
        message.header.type = ClusterMessageType.AWAITING_FENCE
        message.header.require_ack = True
        message.fence_id = fence_id

        self._instance_fence_states[fence_id] = {}
        for instance_id in range(EnvVars.get_instance_count()):
            if instance_id == EnvVars.get_instance_index():
                continue
            self._instance_fence_states[fence_id][instance_id] = False
            self._instance_fence_futures[fence_id] = future

        result = await self.send_and_wait(message)
        if not result.success:
            return result

        return await future

    async def request_state_thread_safe(self, state: ClusterState):
        return await self._execute_coroutine_thread_safe(self.request_state(state))

    async def request_state(self, state: ClusterState):
        message = self._create_request_state_msg(state)
        future = self._state_loop.create_future()
        message_id, pending_msg = self._prepare_request_state_msg(message, future)

        logger.debug("Requesting state: %s with message id: %s", state, message_id)
        self._queue_outgoing_to_instance(message, None)

        result = await pending_msg.future
        if not result.success:
            return result

        result = await self._awaiting_state_resolve_futures[message_id]
        logger.debug("Resolved state: %s message ID: %s", state, message_id)
        return result

    def _create_resolve_state_msg(self, request_state_msg: ClusterRequestState, message_id: int):
        message = ClusterResolvedState()
        message.header.type = ClusterMessageType.RESOLVED_STATE
        message.header.require_ack = True
        message.state = request_state_msg.state
        message.request_message_id = message_id
        return message

    async def resolve_state_thread_safe(self, request_state_msg: ClusterRequestState, message_id: int, instance_id: int | None = None):
        return await self._execute_coroutine_thread_safe(self.resolve_state(request_state_msg, message_id, instance_id))

    async def resolve_state(self, request_state_msg: ClusterRequestState, message_id: int, instance_id: int | None = None):
        return await self.send_and_wait(self._create_resolve_state_msg(request_state_msg, message_id), instance_id)

    def send_no_wait(self, message, instance_id: int | None = None):
        message_id = self._prepare_message(message)
        self._queue_outgoing_to_instance(message, instance_id)
        return message_id

    async def send_and_wait_thread_safe(self, message, instance_id: int | None = None):
        return await self._execute_coroutine_thread_safe(self.send_and_wait(message, instance_id))

    async def send_and_wait(self, message, instance_id: int | None = None):
        if not message.header.require_ack:
            _ = self.send_no_wait(message, instance_id)
            return ACKResult(True)

        message_id = self._prepare_message(message)
        pending_msg = self._create_pending_message(message_id, message, instance_id)

        self._queue_outgoing_to_instance(message, instance_id)

        result = await pending_msg.future
        if not result.success:
            logger.error("No ACK for msg %s: %s", message_id, result.error_msg)

        return result

    def _log_outgoing_queue_size(self):
        outgoing_queue_size = self._outgoing_queue.qsize()
        # if outgoing_queue_size % 100 == 0:
        logger.info('Outgoing message queue size: %s', outgoing_queue_size)

    def _queue_outgoing_to_broadcast(self, packet):
        queued_msg = OutgoingPacket(packet)
        self.queue_outgoing_packet(queued_msg)
        self._log_outgoing_queue_size()

    def _queue_outgoing_to_instance(self, packet, instance_id: int | None):
        if instance_id is None:
            queued_msg = OutgoingPacket(packet)
            self.queue_outgoing_packet(queued_msg)
            return
        addr, direct_port = UDPSingleton.get_cluster_instance_address(instance_id)
        queued_msg = OutgoingPacket(packet, (addr, direct_port))
        self.queue_outgoing_packet(queued_msg)
        self._log_outgoing_queue_size()

    def _queue_outgoing_to_addr(self, packet, addr_and_port: tuple[str, int] | None = None):
        queued_msg = OutgoingPacket(packet, addr_and_port)
        self.queue_outgoing_packet(queued_msg)
        self._log_outgoing_queue_size()

    def get_cached_local_addreses(self):
        if self._local_ips is None:
            interfaces = socket.getaddrinfo(socket.gethostname(), None)
            self._local_ips = [interface[4][0] for interface in interfaces]
            logger.debug("Local IP addresses: %s", self._local_ips)
        return self._local_ips