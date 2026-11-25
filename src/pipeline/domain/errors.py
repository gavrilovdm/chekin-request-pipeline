"""Errors that carry meaning for the pipeline: how a processing failure should be handled."""


class RetryableError(Exception):
    """The attempt failed for a reason that may go away: timeout, 5xx, 429, connection refused."""


class PermanentError(Exception):
    """The attempt failed for a reason that will not change on retry: invalid data, 4xx from the downstream."""


class IllegalTransition(Exception):
    """A status change that the request lifecycle does not allow."""
