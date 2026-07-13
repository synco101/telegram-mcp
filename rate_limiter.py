"""
Rate Limiter for Telegram MCP Server.

Protects against account blocks due to excessive operations:
- DELETE: aggressive limit (10/min) — historically what caused FloodWait/bans
- GET (reads): defense-in-depth limit (30/min) — preemptive cap. Primary defense
  for reads is Telethon's built-in FloodWaitError handling (flood_sleep_threshold);
  this layer just prevents obviously runaway loops.

Strategy: Wait and continue, never fail.
If limit reached -> pause -> continue when allowed.
"""

import sys
import time
import asyncio
from functools import wraps
from typing import Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger("telegram_mcp.rate_limiter")


def announce_wait(kind: str, count: int, limit: int, seconds: float) -> None:
    """Say out loud that we are throttling.

    Not via `logger`: the parent `telegram_mcp` logger AND its handlers both sit at
    ERROR, so info/warning records are dropped. A tool that silently sleeps is
    indistinguishable from a hung one — which is the exact confusion this server
    already cost us once. stderr is where the rest of the server talks.
    """
    print(
        f"{kind} rate limit reached ({count}/{limit}) — throttling {seconds:.1f}s.",
        file=sys.stderr,
    )


@dataclass
class DeleteRateLimitConfig:
    """Configuration for delete rate limiting."""
    # Maximum deletes per minute
    deletes_per_minute: int = 10

    # Delay between deletes in batch (seconds)
    delete_delay: float = 1.0

    # Extra pause when approaching limit (seconds)
    cooldown_pause: float = 10.0

    # Maximum batch size for bulk deletes
    max_batch_size: int = 50


class DeleteRateLimiter:
    """
    Rate limiter specifically for delete operations.

    Key behavior: NEVER fails, only waits.
    If limit reached, pauses until safe to continue.
    """

    def __init__(self, config: Optional[DeleteRateLimitConfig] = None):
        self.config = config or DeleteRateLimitConfig()
        self._delete_times: list = []
        self._lock = asyncio.Lock()
        self._total_deletes = 0
        self._total_waits = 0

    def _cleanup_old_operations(self, window_seconds: int = 60) -> None:
        """Remove operations older than the window."""
        cutoff = time.time() - window_seconds
        self._delete_times = [t for t in self._delete_times if t > cutoff]

    async def wait_if_needed(self) -> float:
        """
        Wait if rate limit is close.

        Returns:
            Wait time in seconds (0 if no wait needed)
        """
        async with self._lock:
            self._cleanup_old_operations()

            current_count = len(self._delete_times)
            limit = self.config.deletes_per_minute

            # If at or above limit, wait for oldest to expire
            if current_count >= limit:
                if self._delete_times:
                    oldest = min(self._delete_times)
                    wait_time = max(0, (oldest + 60) - time.time() + 1)
                else:
                    wait_time = self.config.cooldown_pause

                self._total_waits += 1
                announce_wait("DELETE", current_count, limit, wait_time)

            # If approaching limit (80%), add small delay
            elif current_count >= limit * 0.8:
                wait_time = self.config.delete_delay

            else:
                return 0

        # Sleep OUTSIDE the lock. The previous form released the lock inside its own
        # `async with`, slept, and re-acquired in a `finally` — juggling ownership
        # across an await that the MCP client cancels at 30s. It survives the simple
        # case, but nothing about it is guaranteed under contention, and it is far
        # more code than just not holding the lock while sleeping.
        await asyncio.sleep(wait_time)
        return wait_time

    async def record_delete(self) -> None:
        """Record that a delete operation was performed."""
        async with self._lock:
            self._delete_times.append(time.time())
            self._total_deletes += 1

    async def get_status(self) -> dict:
        """Get current rate limit status."""
        async with self._lock:
            self._cleanup_old_operations()

            return {
                "deletes_last_minute": len(self._delete_times),
                "limit_per_minute": self.config.deletes_per_minute,
                "percent_used": f"{len(self._delete_times) / self.config.deletes_per_minute * 100:.0f}%",
                "total_deletes_session": self._total_deletes,
                "total_waits_session": self._total_waits,
                "config": {
                    "deletes_per_minute": self.config.deletes_per_minute,
                    "delete_delay": self.config.delete_delay,
                    "max_batch_size": self.config.max_batch_size,
                }
            }


# =====================================================================
# GET (read) operations — defense-in-depth preemptive throttle.
# Telethon's FloodWaitError handling is the primary defense; this is a
# safety net that catches runaway loops before they trigger Telegram-side
# rate limits.
# =====================================================================


@dataclass
class GetRateLimitConfig:
    """Configuration for read-operation rate limiting."""
    # This is a runaway-loop guard, not a tax on normal use. A human session reads
    # a few dozen times; a runaway agent reads hundreds. 30/min was low enough that
    # one ordinary chat sweep would trip it and stall real work — set the bar where
    # only genuinely pathological behaviour hits it. Telethon's FloodWaitError
    # remains the primary defense (see module docstring).
    calls_per_minute: int = 100

    # Tiny delay when approaching limit (much lower than DELETE since
    # reads are cheap)
    approach_delay: float = 0.3

    # Hard ceiling on any single throttle sleep. The MCP client gives up on a tool
    # after 30s of silence, so sleeping longer throttles nothing and merely looks
    # like a hang.
    max_wait: float = 20.0


class GetRateLimiter:
    """
    Rate limiter for read operations (get_history, get_messages,
    download_media, search_messages, list_chats, etc.).

    Same NEVER-fail-only-wait contract as DeleteRateLimiter.
    """

    def __init__(self, config: Optional[GetRateLimitConfig] = None):
        self.config = config or GetRateLimitConfig()
        self._call_times: list = []
        self._lock = asyncio.Lock()
        self._total_calls = 0
        self._total_waits = 0

    def _cleanup_old_operations(self, window_seconds: int = 60) -> None:
        cutoff = time.time() - window_seconds
        self._call_times = [t for t in self._call_times if t > cutoff]

    async def wait_if_needed(self) -> float:
        """Block until a slot is available. Returns waited seconds."""
        async with self._lock:
            self._cleanup_old_operations()

            current_count = len(self._call_times)
            limit = self.config.calls_per_minute

            if current_count >= limit:
                if self._call_times:
                    oldest = min(self._call_times)
                    wait_time = max(0, (oldest + 60) - time.time() + 0.5)
                else:
                    wait_time = 1.0

                # Never out-wait the caller. The MCP client aborts a tool after 30s of
                # silence, so a longer sleep doesn't throttle anything — it just
                # reproduces the 30s "hang" this server was broken by. Throttle hard,
                # but stay answerable.
                wait_time = min(wait_time, self.config.max_wait)

                self._total_waits += 1
                announce_wait("GET", current_count, limit, wait_time)

            elif current_count >= limit * 0.8:
                wait_time = self.config.approach_delay

            else:
                return 0

        # Sleep outside the lock — see DeleteRateLimiter.wait_if_needed.
        await asyncio.sleep(wait_time)
        return wait_time

    async def record_call(self) -> None:
        async with self._lock:
            self._call_times.append(time.time())
            self._total_calls += 1

    async def get_status(self) -> dict:
        async with self._lock:
            self._cleanup_old_operations()
            return {
                "calls_last_minute": len(self._call_times),
                "limit_per_minute": self.config.calls_per_minute,
                "percent_used": f"{len(self._call_times) / self.config.calls_per_minute * 100:.0f}%",
                "total_calls_session": self._total_calls,
                "total_waits_session": self._total_waits,
                "config": {
                    "calls_per_minute": self.config.calls_per_minute,
                    "approach_delay": self.config.approach_delay,
                },
            }


# Global instances
_rate_limiter: Optional[DeleteRateLimiter] = None
_get_rate_limiter: Optional[GetRateLimiter] = None


def get_rate_limiter() -> DeleteRateLimiter:
    """Get or create the global DELETE rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = DeleteRateLimiter()
    return _rate_limiter


def get_get_rate_limiter() -> GetRateLimiter:
    """Get or create the global GET rate limiter instance."""
    global _get_rate_limiter
    if _get_rate_limiter is None:
        _get_rate_limiter = GetRateLimiter()
    return _get_rate_limiter


def get_rate_limited(func):
    """
    Decorator for read operations (get_history, download_media, etc.).

    Defense-in-depth: throttles preemptively to ~30 calls/min so
    runaway LLM loops can't burn through Telegram's hidden rate limits.
    Telethon's FloodWaitError handling remains the primary defense.

    Usage:
        @get_rate_limited
        async def get_history(...):
            ...
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        limiter = get_get_rate_limiter()
        wait_time = await limiter.wait_if_needed()
        if wait_time > 0:
            logger.info(f"Resumed after {wait_time:.1f}s GET wait")
        result = await func(*args, **kwargs)
        await limiter.record_call()
        return result
    return wrapper


def delete_rate_limited(func):
    """
    Decorator for delete operations.

    Automatically waits if rate limit is reached, then continues.
    NEVER fails due to rate limiting - only waits.

    Usage:
        @delete_rate_limited
        async def delete_message(...):
            ...
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        limiter = get_rate_limiter()

        # Wait if needed (this will pause, not fail)
        wait_time = await limiter.wait_if_needed()
        if wait_time > 0:
            logger.info(f"Resumed after {wait_time:.1f}s wait")

        # Execute the delete
        result = await func(*args, **kwargs)

        # Record the operation
        await limiter.record_delete()

        return result

    return wrapper


async def delete_messages_batch(
    delete_func,
    chat_id,
    message_ids: list,
    progress_callback=None
) -> dict:
    """
    Delete multiple messages with automatic rate limiting.

    Args:
        delete_func: The actual delete function to call
        chat_id: Chat ID
        message_ids: List of message IDs to delete
        progress_callback: Optional callback(deleted, total, wait_time)

    Returns:
        dict with results: {"deleted": N, "failed": N, "total_time": seconds}
    """
    limiter = get_rate_limiter()
    config = limiter.config

    total = len(message_ids)
    deleted = 0
    failed = 0
    start_time = time.time()

    # Process in batches
    for i in range(0, total, config.max_batch_size):
        batch = message_ids[i:i + config.max_batch_size]

        for msg_id in batch:
            # Wait if needed
            wait_time = await limiter.wait_if_needed()

            try:
                await delete_func(chat_id, msg_id)
                deleted += 1
                await limiter.record_delete()

                if progress_callback:
                    progress_callback(deleted, total, wait_time)

            except Exception as e:
                failed += 1
                logger.warning(f"Failed to delete message {msg_id}: {e}")

            # Small delay between deletes
            await asyncio.sleep(config.delete_delay)

        # Pause between batches
        if i + config.max_batch_size < total:
            logger.info(f"Batch complete. {deleted}/{total} deleted. Pausing...")
            await asyncio.sleep(config.cooldown_pause)

    total_time = time.time() - start_time

    return {
        "deleted": deleted,
        "failed": failed,
        "total": total,
        "total_time_seconds": round(total_time, 1),
        "avg_time_per_delete": round(total_time / max(deleted, 1), 2)
    }


async def get_rate_limit_status() -> dict:
    """Get current rate limiting status — combined DELETE + GET."""
    return {
        "delete": await get_rate_limiter().get_status(),
        "get": await get_get_rate_limiter().get_status(),
    }
