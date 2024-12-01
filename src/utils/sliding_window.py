from dataclasses import dataclass, field
from typing import List, Callable, Any, Optional, Awaitable
import logging
import asyncio
import time
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class RateLimit:
    """
    Class for defining sliding window's rate limits

    :raises ValueError: If the rate limit or time window is less than or equal to 0
    """
    # Rate limit within a specified time window (rate_limit_time_window)
    value: int
    # Time window in seconds for which the rate limit is valid
    time_window: int

    def __post_init__(self):
        if self.value <= 0:
            raise ValueError("Rate limit must be greater than 0")
        if self.time_window <= 0:
            raise ValueError("Rate limit time window must be greater than 0")


@dataclass
class SlidingWindow:
    """Class for defining sliding windows"""
    # Rate limit for this sliding window
    rate_limit: RateLimit
    # Queue to store timestamps of requests
    queue: deque = field(default_factory=deque)
    # Time step for the sliding window
    delta_t: float = field(init=False)

    def __post_init__(self):
        self.delta_t = self.rate_limit.time_window / self.rate_limit.value

    def is_full(self):
        return len(self.queue) >= self.rate_limit.value


async def _check_sliding_window(window: SlidingWindow):
    if window.is_full():
        queue, ratelimit, ratelimit_time_window = window.queue, window.rate_limit.value, window.rate_limit.time_window
        print(f'[>] Sliding window ({ratelimit_time_window}s) size: {len(queue)}/{ratelimit}')
        while len(queue):
            oldest = queue[0]
            time_passed = time.time() - oldest
            # we can remove the oldest element(s)
            if time_passed >= ratelimit_time_window:
                queue.popleft()
            # we exceeded the rate limit, so we have to wait
            elif window.is_full():
                sleep_time = ratelimit_time_window - time_passed + 0.6  # 0.6 to account for some time errors
                print(f"[>] Rate limit exceeded for sliding window ({ratelimit_time_window}s), sleeping for: {sleep_time}")
                await asyncio.sleep(sleep_time)
            # sliding window is now within the limits
            else:
                break
        print(f'[<] Sliding window ({ratelimit_time_window}s) size: {len(queue)}/{ratelimit}')


async def sliding_window(
    *args,
    cb: Callable[..., Awaitable[Any]],
    rate_limits: List[RateLimit],
    delta_t: Optional[float] = None,
    **kwargs
) -> None:
    """
    Sliding window rate-limiting implementation.

    This implementation ensures that a callback function is invoked while adhering 
    to specified rate limits using a sliding window approach. Additional arguments 
    will be passed to the callback function.

    Args:
        cb (Callable[..., Awaitable[Any]]): The awaitable callback function to be called.
        rate_limits (List[RateLimit]): A list of RateLimit objects defining the rate limits.
        delta_t (Optional[float]): The time step (in seconds) for the sliding windows. 
            Defaults to None and will be precalculated based on the slowest rate limit.

    Returns:
        NoReturn: This function does not return anything.

    Example:
        ```python
        from sliding_window import sliding_window, RateLimit

        async def main():
            try:
                async def my_epic_request(param1, param2):
                    print(f'Request: {param1}, {param2}')

                # 20 requests per second, 50 requests per minute
                rate_limits = [RateLimit(value=20, time_window=1), RateLimit(value=50, time_window=60)]

                await sliding_window('PARAM_1', cb=my_epic_request, 
                    rate_limits=rate_limits, param2='PARAM_2')
            except ValueError as e:
                print(f"An error occurred: {e}")

        # OUT:
        # Request: PARAM_1, PARAM_2
        # Request: PARAM_1, PARAM_2
        # ...
        ```
    """
    # META
    counter = 0

    # SLIDING WINDOWS
    sliding_windows: List[SlidingWindow] = [SlidingWindow(rate_limit) for rate_limit in rate_limits]

    # DETERMINE THE TIME STEP (delta_t) FOR THE SLIDING WINDOWS
    # NOTE:
    # We could choose different strategies here, such as fetching as fast as possible
    # by setting delta_t to the minimum of all rate limits
    if delta_t is None:
        delta_t = max([window.delta_t for window in sliding_windows])  # try to distribute the requests evenly over the time windows

    # MAIN LOOP
    print(f"[.] Sliding windows delta_t: {delta_t}")
    while True:
        for window in sliding_windows:
            await _check_sliding_window(window)

        if all(not window.is_full() for window in sliding_windows):
            counter += 1
            print(f"[+] Calling {cb.__name__}, iteration: {counter}")
            await cb(*args, **kwargs)
            now = time.time()
            for window in sliding_windows:
                window.queue.append(now)
            await asyncio.sleep(delta_t)
