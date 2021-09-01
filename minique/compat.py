try:
    from typing import TYPE_CHECKING  # "New in version 3.5.2."
except ImportError:  # pragma: no cover
    TYPE_CHECKING = False

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None  # type: ignore

__all__ = [
    "sentry_sdk",
    "TYPE_CHECKING",
]
