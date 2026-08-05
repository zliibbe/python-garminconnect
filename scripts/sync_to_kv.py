#!/usr/bin/env python3
"""Fetch Garmin Connect data and write it to Upstash/Vercel KV.

Run on a schedule (see ``.github/workflows/sync-garmin-kv.yml``) to feed the
``/me/health`` dashboard on zachliibbe.com. This script never talks to
anything but Garmin and Upstash — the Next.js site only ever reads what this
script already wrote.

Auth: resumes the token cached at the ``garmin:tokens`` KV key. There is no
fresh-credential/MFA fallback here on purpose — GitHub Actions can't answer
an interactive MFA prompt. If login fails with ``GarminConnectAuthenticationError``,
run ``scripts/bootstrap_garmin_token.py`` locally to reseed the token.

Usage::

    python scripts/sync_to_kv.py [--categories sleep] [--backfill-days 3] [--dry-run]

Required env vars: GARMIN_EMAIL, GARMIN_PASSWORD (fallback only),
UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN.

One-time historical seed: the scheduled run only backfills the last few days
(``--backfill-days``, default 3), so charts start sparse. After a new
category ships, seed ~90-120 days of history once by running this locally
(not via the GitHub Actions workflow — its 10-minute timeout and Garmin's
rate limits don't mix well with hundreds of sequential API calls)::

    python scripts/sync_to_kv.py --categories sleep --backfill-days 100

RETENTION_DAYS (below) trims anything older than ~13 months on every run, so
there's no reason to seed further back than that.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import sys
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, cast

from _garmin_utils import safe_api_call
from upstash_redis import Redis

from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectTooManyRequestsError,
)

if TYPE_CHECKING:
    from collections.abc import Callable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("garmin_sync")

DEFAULT_BACKFILL_DAYS = 3
RETENTION_DAYS = int(os.getenv("GARMIN_KV_RETENTION_DAYS", "400"))


def token_key() -> str:
    return "garmin:tokens"


def day_key(category: str, d: date) -> str:
    return f"garmin:{category}:day:{d.isoformat()}"


def series_key(category: str) -> str:
    return f"garmin:{category}:series"


def latest_key(category: str) -> str:
    return f"garmin:{category}:latest"


def get_redis() -> Redis:
    url = os.environ["UPSTASH_REDIS_REST_URL"]
    token = os.environ["UPSTASH_REDIS_REST_TOKEN"]
    return Redis(url=url, token=token)


def init_garmin(redis: Redis) -> Garmin:
    """Log in, preferring the cached KV token over a fresh credential login."""
    token_json = redis.get(token_key())
    garmin = Garmin(
        email=os.getenv("GARMIN_EMAIL"), password=os.getenv("GARMIN_PASSWORD")
    )
    if token_json:
        garmin.login(tokenstore=token_json)
    else:
        garmin.login()
    return garmin


def save_tokens(redis: Redis, garmin: Garmin) -> None:
    """Persist the (possibly refreshed) token back to KV.

    Loading tokens via the in-memory string branch never sets
    ``_tokenstore_path``, so the library's own auto-dump-on-refresh is a
    no-op for us — this call is the only thing that keeps the cached token
    from going stale across runs.
    """
    redis.set(token_key(), garmin.client.dumps())


def write_daily_snapshot(
    redis: Redis, category: str, d: date, blob: dict[str, Any]
) -> None:
    payload = json.dumps(blob)
    redis.set(day_key(category, d), payload)
    redis.zadd(series_key(category), {d.isoformat(): int(d.strftime("%Y%m%d"))})
    redis.set(latest_key(category), payload)


def trim_series(redis: Redis, category: str, retention_days: int) -> None:
    cutoff = date.today() - timedelta(days=retention_days)
    cutoff_score = int(cutoff.strftime("%Y%m%d"))
    old_dates = cast(
        "list[str]",
        redis.zrange(series_key(category), 0, cutoff_score, sortby="BYSCORE"),
    )
    if not old_dates:
        return
    day_keys = [day_key(category, date.fromisoformat(d)) for d in old_dates]
    redis.delete(*day_keys)
    redis.zremrangebyscore(series_key(category), 0, cutoff_score)
    logger.info(
        "Trimmed %d old %s snapshot(s) older than %s",
        len(old_dates),
        category,
        cutoff.isoformat(),
    )


def sync_sleep_recovery(garmin: Garmin, d: date) -> dict[str, Any] | None:
    """Sleep, body battery, stress, HRV, and resting HR for one day."""
    cdate = d.isoformat()
    blob: dict[str, Any] = {"date": cdate}
    any_success = False

    calls: list[tuple[str, Callable[..., Any], tuple[Any, ...]]] = [
        ("sleep", garmin.get_sleep_data, (cdate,)),
        ("body_battery", garmin.get_body_battery, (cdate, cdate)),
        ("stress", garmin.get_all_day_stress, (cdate,)),
        ("hrv", garmin.get_hrv_data, (cdate,)),
        ("resting_hr", garmin.get_rhr_day, (cdate,)),
    ]
    for field, api_method, args in calls:
        ok, result, err = safe_api_call(api_method, *args)
        if ok and result is not None:
            blob[field] = result
            any_success = True
        elif err:
            logger.warning("sleep %s: %s failed: %s", cdate, field, err)

    return blob if any_success else None


def sync_daily_activity(garmin: Garmin, d: date) -> dict[str, Any] | None:
    """Steps, calories, distance, and floors for one day."""
    cdate = d.isoformat()
    blob: dict[str, Any] = {"date": cdate}
    any_success = False

    calls: list[tuple[str, Callable[..., Any], tuple[Any, ...]]] = [
        ("summary", garmin.get_user_summary, (cdate,)),
        ("steps", garmin.get_steps_data, (cdate,)),
        ("floors", garmin.get_floors, (cdate,)),
    ]
    for field, api_method, args in calls:
        ok, result, err = safe_api_call(api_method, *args)
        if ok and result is not None:
            blob[field] = result
            any_success = True
        elif err:
            logger.warning("activity %s: %s failed: %s", cdate, field, err)

    return blob if any_success else None


# Categories are added one phase at a time (see the plan's build order):
# sleep (Phase 1) -> activity (Phase 2) -> activities (Phase 3) -> training (Phase 4).
CATEGORIES: dict[str, Callable[[Garmin, date], dict[str, Any] | None]] = {
    "sleep": sync_sleep_recovery,
    "activity": sync_daily_activity,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--categories",
        default="sleep",
        help=f"Comma-separated categories to sync. Available: {', '.join(CATEGORIES)}",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=DEFAULT_BACKFILL_DAYS,
        help="Re-sync the most recent N days (Garmin finalizes some metrics hours after the fact).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but skip all KV writes (including the token write-back).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    requested = [c.strip() for c in args.categories.split(",") if c.strip()]
    unknown = [c for c in requested if c not in CATEGORIES]
    if unknown:
        logger.error(
            "Unknown categories: %s. Available: %s",
            ", ".join(unknown),
            ", ".join(CATEGORIES),
        )
        return 1

    redis = get_redis()
    garmin: Garmin | None = None
    had_failure = False

    try:
        garmin = init_garmin(redis)
    except GarminConnectAuthenticationError as err:
        logger.exception(
            "Authentication failed: %s. Run scripts/bootstrap_garmin_token.py to reseed the token.",
            err,
        )
        return 1
    except GarminConnectTooManyRequestsError as err:
        logger.exception("Rate limited during login: %s", err)
        return 1

    try:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(args.backfill_days)]

        for category in requested:
            sync_fn = CATEGORIES[category]
            successes = 0
            for d in dates:
                try:
                    blob = sync_fn(garmin, d)
                except GarminConnectAuthenticationError as err:
                    logger.exception("Authentication failed mid-run: %s", err)
                    had_failure = True
                    break
                except GarminConnectTooManyRequestsError as err:
                    logger.exception("Rate limited mid-run: %s", err)
                    had_failure = True
                    break

                if blob is None:
                    logger.warning("%s %s: no data available", category, d.isoformat())
                    had_failure = True
                    continue

                if not args.dry_run:
                    write_daily_snapshot(redis, category, d, blob)
                successes += 1
                logger.info("%s %s: synced", category, d.isoformat())

            if not args.dry_run:
                trim_series(redis, category, RETENTION_DAYS)

            logger.info("%s: %d/%d date(s) synced", category, successes, len(dates))
            if successes == 0:
                had_failure = True

        logger.info(
            "Sync complete: categories=%s dates=%s..%s",
            ",".join(requested),
            dates[-1].isoformat(),
            dates[0].isoformat(),
        )
        return 1 if had_failure else 0
    finally:
        if garmin is not None and not args.dry_run:
            with contextlib.suppress(Exception):
                save_tokens(redis, garmin)


if __name__ == "__main__":
    sys.exit(main())
