from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .state_handler import StateHandler


class StateResult:
    def __init__(
        self,
        last_state: int,
        last_state_handler: "StateHandler",
        next_state: int | None = None,
        next_state_handler: "StateHandler" = None,
    ):
        self.last_state = last_state
        self.last_state_handler = last_state_handler
        self.next_state = next_state if next_state is not None else last_state
        if (
            next_state is not None
            and next_state != last_state
            and next_state_handler is None
        ):
            raise ValueError(
                "next_state_handler cannot be None when transitioning to a new state"
            )
        self.next_state_handler = (
            next_state_handler
            if next_state_handler is not None
            else last_state_handler
        )
