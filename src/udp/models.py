"""
Domain models for UDP networking.
"""
from dataclasses import dataclass
from typing import Optional, Dict, List, Any, Union


@dataclass
class NetworkAddress:
    """
    Represents a network address with host and port.
    """
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"
    
    def to_tuple(self) -> tuple[str, int]:
        """Convert to a tuple representation (host, port)."""
        return (self.host, self.port)
    
    @classmethod
    def from_tuple(cls, addr_tuple: tuple[str, int]) -> 'NetworkAddress':
        """Create a NetworkAddress from a tuple (host, port)."""
        return cls(addr_tuple[0], addr_tuple[1])


@dataclass
class InstanceAddress:
    """
    Represents an instance address with instance ID and network address.
    """
    instance_id: int
    address: NetworkAddress
    
    def __str__(self) -> str:
        return f"Instance {self.instance_id} at {self.address}"
    
    @classmethod
    def from_tuple(cls, addr_tuple: tuple[int, str, int]) -> 'InstanceAddress':
        """Create an InstanceAddress from a tuple (instance_id, host, port)."""
        return cls(
            instance_id=addr_tuple[0],
            address=NetworkAddress(addr_tuple[1], addr_tuple[2])
        )


@dataclass
class MessageHeader:
    """
    Represents a message header.
    """
    message_id: int
    sender_instance_id: int
    message_type: str
    require_ack: bool = False
    expected_key: int = -1

    def __str__(self) -> str:
        return (
            f"MessageHeader(id={self.message_id}, "
            f"sender={self.sender_instance_id}, "
            f"type={self.message_type}, "
            f"require_ack={self.require_ack})"
        )


@dataclass
class NetworkMessage:
    """
    Represents a network message.
    """
    header: MessageHeader
    payload: Any
    sender_address: Optional[NetworkAddress] = None

    def __str__(self) -> str:
        return f"Message({self.header.message_type}) from {self.sender_address}"


@dataclass
class NetworkBuffer:
    """
    Represents a network buffer.
    """
    sender_instance_id: int
    chunk_id: int
    data: bytes
    sender_address: Optional[NetworkAddress] = None

    def __str__(self) -> str:
        return f"Buffer(chunk={self.chunk_id}, size={len(self.data)}) from instance {self.sender_instance_id}"