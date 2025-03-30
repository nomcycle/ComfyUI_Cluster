
from ..log import logger
from ..instance import ThisInstance
from ..queued import IncomingMessage, IncomingBuffer


class StateHandler:
    from .state_result import StateResult

    def __init__(
        self,
        instance: ThisInstance,
        expected_state: int,
        expected_message_types: int,
    ):
        self._instance = instance
        self._expected_state = expected_state
        self._expected_message_types = expected_message_types

    def check_current_state(self, current_state: int):
        # logger.debug("State check: current=%d expected=%d", current_state, self._expected_state)
        if current_state != self._expected_state and not (
            current_state & self._expected_state
        ):
            logger.error(
                f"Invalid state: {current_state} not in {self._expected_state}"
            )
            return False
        return True

    def check_message_type(self, msg_type: int):
        # logger.debug("Message type check: type=%d expected=%d", msg_type, self._expected_message_types)
        if msg_type != self._expected_message_types and not (
            msg_type & self._expected_message_types
        ):
            logger.warning(
                "Invalid message type: %d not in %d",
                msg_type,
                self._expected_message_types,
            )
            return False
        return True

    async def handle_state(self, current_state: int) -> StateResult | None:
        raise NotImplementedError("handle_state not implemented")

    async def handle_message(
        self, current_state: int, incoming_message: IncomingMessage
    ) -> StateResult | None:
        raise NotImplementedError("handle_message not implemented")

    async def handle_buffer(
        self, current_state: int, incoming_buffer: IncomingBuffer
    ) -> StateResult | None:
        raise NotImplementedError("handle_buffer not implemented")
