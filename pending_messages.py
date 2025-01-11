import time
from typing import Dict

class PendingInstanceMessage:
    def __init__(self, timestamp: float, retry_count: int, addr: str, instance_id: int | None = None):
        self.timestamp: float = timestamp
        self.retry_count: int = retry_count
        self.addr: str = addr
        self.instance_id: int | None = instance_id

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