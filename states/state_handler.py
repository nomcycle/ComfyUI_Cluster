from typing import Dict

from ..log import logger
from ..instance import ThisInstance

class StateHandler:
    from .state_result import StateResult
    def __init__(self, instance: ThisInstance, expected_state: int, expected_message_types: int):
        self._instance = instance
        self._expected_state = expected_state
        self._expected_message_types = expected_message_types

    def check_current_state(self, current_state: int):
        logger.debug("State check: current=%d expected=%d", current_state, self._expected_state)
        if current_state != self._expected_state and not (current_state & self._expected_state):
            raise ValueError(f"Invalid state: {current_state} not in {self._expected_state}")
            return False
        return True

    def check_message_type(self, message_type: int):
        logger.debug("Message type check: type=%d expected=%d", message_type, self._expected_message_types)
        if message_type != self._expected_message_types and not (message_type & self._expected_message_types):
            logger.warning("Invalid message type: %d not in %d", message_type, self._expected_message_types)
            return False
        return True

    async def handle_state(self, current_state) -> StateResult | None:
        raise NotImplementedError("handle_state not implemented")

    def handle_message(self, current_state, message, addr) -> StateResult | None:
        raise NotImplementedError("handle_message not implemented")