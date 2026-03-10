"""MFL API exception hierarchy."""


class MFLError(Exception):
    """Base exception for MFL API errors."""


class MFLAuthError(MFLError):
    """Authentication failure with the MFL API."""


class MFLRateLimitError(MFLError):
    """Rate limit exceeded (HTTP 429) from the MFL API."""


class MFLAPIError(MFLError):
    """Unexpected error from the MFL API."""

    def __init__(self, status_code: int, response_text: str) -> None:
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(f"MFL API error {status_code}: {response_text}")
