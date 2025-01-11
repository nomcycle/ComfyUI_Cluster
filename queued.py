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

class OutgoingPacket:
    def __init__(self, packet, optional_addr: str = None):
        self.packet = packet
        self.optional_addr: str = optional_addr