"""Shared helpers for the KV-sync scripts.

Extracted from ``example.py``'s inline ``safe_api_call`` so it can be
imported from both ``bootstrap_garmin_token.py`` and ``sync_to_kv.py``
without duplicating the Garmin exception-to-message mapping.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from garminconnect import (
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
)

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger("garmin_sync")

_STATUS_MESSAGES = {
    "400": "Not available (400) — feature may not be enabled for this account",
    "401": "Authentication required (401)",
    "403": "Access denied (403)",
    "404": "Not found (404) — endpoint may have moved",
    "429": "Rate limit (429)",
    "500": "Server error (500)",
}


def safe_api_call(
    api_method: Callable[..., Any], *args: Any, **kwargs: Any
) -> tuple[bool, Any, str | None]:
    """Call a Garmin API method and return (success, result, error_message).

    Mirrors the error classification in ``example.py``'s helper of the same
    name: authentication and rate-limit errors are raised to the caller
    unchanged (they should abort the whole run), everything else is reduced
    to a human-readable message so a single missing endpoint doesn't take
    down an entire category's sync.
    """
    try:
        result = api_method(*args, **kwargs)
        return True, result, None

    except GarminConnectAuthenticationError:
        raise
    except GarminConnectTooManyRequestsError:
        raise
    except GarminConnectConnectionError as e:
        error_str = str(e)
        message = next(
            (msg for code, msg in _STATUS_MESSAGES.items() if code in error_str),
            f"Connection error: {e}",
        )
        return False, None, message
    except Exception as e:
        return False, None, f"Unexpected error: {e}"
