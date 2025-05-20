"""
Custom exceptions for UDP networking.
"""


class UDPNetworkError(Exception):
    """Base exception for UDP networking errors."""
    pass


class MessageValidationError(UDPNetworkError):
    """Exception raised when a message fails validation."""
    pass


class MessageProcessingError(UDPNetworkError):
    """Exception raised when message processing fails."""
    pass


class MessageSendError(UDPNetworkError):
    """Exception raised when sending a message fails."""
    pass


class MessageTimeoutError(UDPNetworkError):
    """Exception raised when waiting for a message times out."""
    pass


class BufferValidationError(UDPNetworkError):
    """Exception raised when a buffer fails validation."""
    pass


class BufferProcessingError(UDPNetworkError):
    """Exception raised when buffer processing fails."""
    pass


class BufferSendError(UDPNetworkError):
    """Exception raised when sending a buffer fails."""
    pass


class SynchronizationError(UDPNetworkError):
    """Exception raised when synchronization fails."""
    pass


class FenceError(SynchronizationError):
    """Exception raised when a fence operation fails."""
    pass


class StateResolveError(SynchronizationError):
    """Exception raised when state resolution fails."""
    pass


class ExpectedMessageError(UDPNetworkError):
    """Exception raised when an expected message operation fails."""
    
    def __init__(self, message="Expected message operation failed", expected_key=None):
        self.expected_key = expected_key
        super().__init__(
            f"{message} (expected_key={expected_key})" if expected_key is not None else message
        )