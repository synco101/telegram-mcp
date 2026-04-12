"""
Rate Limiter for Telegram MCP Server - DELETE operations only.
Protects against account blocks due to excessive delete operations.

Strategy: Wait and continue, never fail.
If limit reached -> pause -> continue when allowed.
"""

import time
import asyncio
from functools import wraps
from typing import Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger("telegram_mcp.rate_limiter")


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

                logger.info(f"Rate limit reached ({current_count}/{limit}). Waiting {wait_time:.1f}s...")
                self._total_waits += 1

                # Release lock during wait
                self._lock.release()
                try:
                    await asyncio.sleep(wait_time)
                finally:
                    await self._lock.acquire()

                # Cleanup again after wait
                self._cleanup_old_operations()
                return wait_time

            # If approaching limit (80%), add small delay
            elif current_count >= limit * 0.8:
                wait_time = self.config.delete_delay
                logger.debug(f"Approaching limit ({current_count}/{limit}). Small delay {wait_time}s")

                self._lock.release()
                try:
                    await asyncio.sleep(wait_time)
                finally:
                    await self._lock.acquire()

                return wait_time

            return 0

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


# Global instance
_rate_limiter: Optional[DeleteRateLimiter] = None


def get_rate_limiter() -> DeleteRateLimiter:
    """Get or create the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = DeleteRateLimiter()
    return _rate_limiter


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
    """Get current rate limiting status."""
    return await get_rate_limiter().get_status()
