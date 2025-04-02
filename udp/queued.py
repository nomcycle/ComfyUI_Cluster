from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterMessageType,
    ClusterAck,
    ClusterSignalHotReload,
    ClusterRequestState,
    ClusterAwaitingFence,
    ClusterResolvedState,
    ClusterAnnounceInstance,
    ClusterDistributePrompt,
    ClusterDistributeBufferBegin,
    ClusterDistributeBufferAck,
    ClusterDistributeBufferResend,
)


class IncomingPacket:
    def __init__(self, packet, sender_addr):
        self.packet = packet
        self.sender_addr: str = sender_addr

    def get_is_buffer(self) -> bool:
        return False

    @staticmethod
    def is_buffer(packet):
        return (
            len(packet) >= 4
            and int.from_bytes(packet[:4], byteorder="big") == 123456789
        )


class IncomingBuffer(IncomingPacket):
    def __init__(self, packet, sender_addr):
        super().__init__(packet, sender_addr)
        self._cached_sender_instance_id = int.from_bytes(
            self.packet[4:8], byteorder="big"
        )
        self._cached_chunk_id = int.from_bytes(
            self.packet[8:12], byteorder="big"
        )

    def get_sender_instance_id(self) -> int:
        return self._cached_sender_instance_id

    def get_chunk_id(self) -> int:
        return self._cached_chunk_id

    def get_is_buffer(self) -> bool:
        return True


class IncomingMessage:
    def __init__(
        self,
        sender_addr: str,
        sender_instance_id: int,
        msg_type_str: str,
        message_id: int,
        msg_type: int,
        require_ack: bool,
        expected_key: int,
        message,
    ):
        self.sender_addr = sender_addr
        self.sender_instance_id = sender_instance_id
        self.msg_type_str = msg_type_str
        self.message_id = message_id
        self.msg_type = msg_type
        self.require_ack = require_ack
        self.expected_key = expected_key
        self.message = message

    def __str__(self):
        header = (
            f"\tClusterMessageHeader:\n"
            f"\t\tsender_addr={self.sender_addr},\n"
            f"\t\tsender_instance_id={self.sender_instance_id},\n"
            f"\t\tmsg_type={self.msg_type_str},\n"
            f"\t\tmessage_id={self.message_id},\n"
            f"\t\trequire_ack={self.require_ack},\n"
            f"\t\texpected_key={self.expected_key}"
        )

        if self.msg_type == ClusterMessageType.ACK:
            ack = ParseDict(self.message, ClusterAck())
            return (
                f"\nClusterAck:\n"
                f"\tack_message_id={ack.ack_message_id},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.SIGNAL_HOT_RELOAD:
            hot_reload = ParseDict(self.message, ClusterSignalHotReload())
            return (
                f"\nClusterSignalHotReload:\n"
                f"\ttimestamp={hot_reload.timestamp},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.ANNOUNCE:
            announce_instance = ParseDict(
                self.message, ClusterAnnounceInstance()
            )
            return (
                f"\nClusterAnnounceInstance:\n"
                f"\trole={announce_instance.role},\n"
                f"\tall_accounted_for={announce_instance.all_accounted_for},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_PROMPT:
            distribute_prompt = ParseDict(
                self.message, ClusterDistributePrompt()
            )
            return (
                f"\nClusterDistributePrompt:\n"
                f"\tprompt={distribute_prompt.prompt},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.AWAITING_FENCE:
            distribute_prompt = ParseDict(self.message, ClusterAwaitingFence())
            return (
                f"\nClusterAwaitingFence:\n"
                f"\tfence_id={distribute_prompt.fence_id},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.REQUEST_STATE:
            request_state = ParseDict(self.message, ClusterRequestState())
            return (
                f"\nClusterRequestState:\n"
                f"\tstate={request_state.state},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.RESOLVED_STATE:
            resolve_state = ParseDict(self.message, ClusterResolvedState())
            return (
                f"\nClusterResolveState:\n"
                f"\tstate={resolve_state.state},\n"
                f"\trequest_message_id={resolve_state.request_message_id},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN:
            buffer_begin = ParseDict(
                self.message, ClusterDistributeBufferBegin()
            )
            return (
                f"\nClusterDistributeBufferBegin:\n"
                f"\tinstance_index={buffer_begin.instance_index},\n"
                f"\tbuffer_type={buffer_begin.buffer_type},\n"
                f"\tbuffer_byte_size={buffer_begin.buffer_byte_size},\n"
                f"\tchunk_count={buffer_begin.chunk_count},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_NEXT:
            distribute_prompt = ParseDict(
                self.message, ClusterDistributePrompt()
            )
            return f"\nClusterDistributeBufferNext:\n{header}"
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_RESEND:
            buffer_resend = ParseDict(
                self.message, ClusterDistributeBufferResend()
            )
            return (
                f"\nClusterDistributeBufferResend:\n"
                f"\tinstance_index={buffer_resend.instance_index},\n"
                # f"\tmissing_chunk_ids={buffer_resend.missing_chunk_ids},\n"
                f"{header}"
            )
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
            buffer_ack = ParseDict(self.message, ClusterDistributeBufferAck())
            return (
                f"\nClusterDistributeBufferAck:\n"
                f"\tinstance_index={buffer_ack.instance_index},\n"
                f"{header}"
            )
        else:
            return f"Unable to pretty print message, unknown message type: {self.msg_type_str}"


class OutgoingPacket:
    def __init__(self, packet, optional_addr: tuple[str, int] | None = None):
        self.packet = packet
        self.optional_addr: tuple[str, int] | None = optional_addr
