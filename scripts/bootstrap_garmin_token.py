#!/usr/bin/env python3
"""One-time (and occasional) interactive Garmin login, seeding tokens into KV.

GitHub Actions can't prompt for an MFA code, so ``sync_to_kv.py`` only ever
*resumes* a cached token from KV — it never performs a fresh credential
login. This script is the escape hatch: run it locally whenever there's no
token in KV yet, or whenever the scheduled sync starts failing with
``GarminConnectAuthenticationError`` (the refresh token expired or was
revoked).

Usage::

    export GARMIN_EMAIL=you@example.com
    export GARMIN_PASSWORD=...            # or leave unset to be prompted
    export UPSTASH_REDIS_REST_URL=...
    export UPSTASH_REDIS_REST_TOKEN=...
    python scripts/bootstrap_garmin_token.py

Do not commit the env vars above anywhere — export them by hand or source
a gitignored ``.env`` file.
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
from getpass import getpass

from upstash_redis import Redis

from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectTooManyRequestsError,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("garmin_sync")

TOKEN_KEY = (
    "garmin:tokens"  # noqa: S105  # nosec B105 -- a KV key name, not a credential
)


def get_redis() -> Redis:
    url = os.environ["UPSTASH_REDIS_REST_URL"]
    token = os.environ["UPSTASH_REDIS_REST_TOKEN"]
    return Redis(url=url, token=token)


def main() -> int:
    redis = get_redis()

    existing = redis.get(TOKEN_KEY)
    if existing:
        print(
            "A token already exists at garmin:tokens. Re-running will "
            "overwrite it with a fresh credential login."
        )
        if input("Continue? [y/N] ").strip().lower() != "y":
            return 0

    email = os.getenv("GARMIN_EMAIL") or input("Garmin email: ").strip()
    password = os.getenv("GARMIN_PASSWORD") or getpass("Garmin password: ")

    with contextlib.suppress(KeyboardInterrupt):
        try:
            garmin = Garmin(
                email=email,
                password=password,
                prompt_mfa=lambda: input("MFA code: ").strip(),
            )
            garmin.login()
        except GarminConnectTooManyRequestsError as err:
            logger.exception("Rate limit: %s", err)
            return 1
        except GarminConnectAuthenticationError as err:
            logger.exception("Login failed: %s", err)
            return 1

        token_json = garmin.client.dumps()
        redis.set(TOKEN_KEY, token_json)
        logger.info("Login successful. Token written to %s in KV.", TOKEN_KEY)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
