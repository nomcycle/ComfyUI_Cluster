from google.protobuf.json_format import ParseDict
from .protobuf.messages_pb2 import (
    ClusterMessageType,
    ClusterMessageHeader,
    ClusterAck,
    ClusterSignalHotReload,
    ClusterFenceRequest,
    ClusterFenceResponse,
    ClusterAnnounceInstance,
    ClusterDistributePrompt,
    ClusterBufferType,
    ClusterDistributeBufferBegin,
    ClusterDistributeBufferAck,
    ClusterDistributeBufferAllSent,
    ClusterDistributeBufferResend,
    ClusterSignalIdle,
    ClusterRole
)

class IncomingPacket:
    def _is_buffer_packet (self, data):
        if len(data) >= 4:
            magic_number = int.from_bytes(data[:4], byteorder='big')
            if magic_number == 123456789:
                return True
        return False

    def __init__(self, packet, sender_addr):
        self.packet = packet
        self.is_buffer = self._is_buffer_packet(self.packet)
        self.sender_addr: str = sender_addr

    def get_is_buffer(self):
        return self.is_buffer

class IncomingMessage:
    def __init__(self, 
                sender_addr: str,
                sender_instance_id: int,
                msg_type_str: str,
                message_id: int,
                msg_type: int,
                process_id: int,
                require_ack: bool,
                message):
        self.sender_addr = sender_addr
        self.sender_instance_id = sender_instance_id
        self.msg_type_str = msg_type_str
        self.message_id = message_id
        self.msg_type = msg_type
        self.process_id = process_id
        self.require_ack = require_ack
        self.message = message

    def __str__(self):
        header = (f"\tClusterMessageHeader:\n"
                 f"\t\tsender_addr={self.sender_addr},\n"
                 f"\t\tsender_instance_id={self.sender_instance_id},\n"
                 f"\t\tmsg_type={self.msg_type_str},\n"
                 f"\t\tmessage_id={self.message_id},\n"
                 f"\t\tprocess_id={self.process_id},\n"
                 f"\t\trequire_ack={self.require_ack}")

        if self.msg_type == ClusterMessageType.ACK:
            ack = ParseDict(self.message, ClusterAck())
            return (f"\nClusterAck:\n"
                   f"\tack_message_id={ack.ack_message_id},\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.SIGNAL_HOT_RELOAD:
            hot_reload = ParseDict(self.message, ClusterSignalHotReload())
            return (f"\nClusterSignalHotReload:\n"
                   f"\ttimestamp={hot_reload.timestamp},\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.ANNOUNCE:
            announce_instance = ParseDict(self.message, ClusterAnnounceInstance())
            return (f"\nClusterAnnounceInstance:\n"
                   f"\trole={announce_instance.role},\n"
                   f"\tall_accounted_for={announce_instance.all_accounted_for},\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_PROMPT:
            distribute_prompt = ParseDict(self.message, ClusterDistributePrompt())
            return (f"\nClusterDistributePrompt:\n"
                   f"\tprompt={distribute_prompt.prompt},\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN:
            buffer_begin = ParseDict(self.message, ClusterDistributeBufferBegin())
            return (f"\nClusterDistributeBufferBegin:\n"
                   f"\tinstance_index={buffer_begin.instance_index},\n"
                   f"\tbuffer_type={buffer_begin.buffer_type},\n"
                   f"\tbuffer_byte_size={buffer_begin.buffer_byte_size},\n"
                   f"\tchunk_count={buffer_begin.chunk_count},\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT:
            buffer_all_sent = ParseDict(self.message, ClusterDistributeBufferAllSent())
            return (f"\nClusterDistributeBufferAllSent:\n"
                   f"\tinstance_index={buffer_all_sent.instance_index},\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_NEXT:
            distribute_prompt = ParseDict(self.message, ClusterDistributePrompt())
            return (f"\nClusterDistributeBufferNext:\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_RESEND:
            buffer_resend = ParseDict(self.message, ClusterDistributeBufferResend())
            return (f"\nClusterDistributeBufferResend:\n"
                   f"\tinstance_index={buffer_resend.instance_index},\n"
                   # f"\tmissing_chunk_ids={buffer_resend.missing_chunk_ids},\n"
                   f"{header}")
        elif self.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
            buffer_ack = ParseDict(self.message, ClusterDistributeBufferAck())
            return (f"\nClusterDistributeBufferAck:\n"
                   f"\tinstance_index={buffer_ack.instance_index},\n"
                   f"{header}")
        else:
            return f"Unable to pretty print message, unknown message type: {self.msg_type_str}"

class OutgoingPacket:
    def __init__(self, packet, optional_addr: str = None):
        self.packet = packet
        self.optional_addr: str = optional_addr