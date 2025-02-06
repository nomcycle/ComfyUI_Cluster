import time
from typing import Dict

class PendingInstanceMessage:
    def __init__(self, timestamp: float, retry_count: int, instance_id: int | None = None):
        self.timestamp: float = timestamp
        self.retry_count: int = retry_count
        self.instance_id: int | None = instance_id

class PendingMessage:
    def __init__(self, message_id: int, message):
        self.message_id: int = message_id
        self.message = message
        self.pending_acks: Dict[int, PendingInstanceMessage] = {} # Dict of addr -> {timestamp: float, retry_count: int}
        self.future = None
        self.MAX_RETRIES: int = 100

    def increment_retry(self, instance_id: int):
        if instance_id not in self.pending_acks:
            self.pending_acks[instance_id] = PendingInstanceMessage(time.time(), 0, instance_id)
        self.pending_acks[instance_id].retry_count += 1
        self.pending_acks[instance_id].timestamp = time.time()
        return self.pending_acks[instance_id].retry_count

    def has_exceeded_retries(self, instance_id: int):
        if instance_id not in self.pending_acks:
            return False
        return self.pending_acks[instance_id].retry_count >= self.MAX_RETRIES
        
    def should_retry(self, instance_id: int, current_time: float):
        if instance_id not in self.pending_acks:
            return False
        last_try = self.pending_acks[instance_id].timestamp
        return current_time - last_try > 5.0 