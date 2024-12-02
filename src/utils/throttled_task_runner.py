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
class _SlidingWindow:
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


class ThrottledTaskRunner:
    """
    Sliding window based rate-limiting.

    Typical usage could be API endpoints with defined rate limits. For example, you can
    have a rate limit of 20 requests per second and 50 requests per minute.

    This class allows you to define rate limits and run arbitrary code while adhering
    to those specific rate limits using a sliding window approach. The approach ensures 
    that the rate limits are not exceeded over the specified time windows while allowing 
    for "bursts" of requests by choosing small time step. **By default, the time step (`delta_t`) 
    is precalculated based on the slowest rate limit, so the requests are distributed 
    evenly over the time windows**.

    Example:

    ```python
    from throttled_task_runner import ThrottledTaskRunner, RateLimit

    async def main():
        try:
            rate_limits = [
                RateLimit(value=20, time_window=1),
                RateLimit(value=50, time_window=60)
            ]

            ttr = ThrottledTaskRunner(rate_limits=rate_limits, delta_t=0.1)

            async def some_request(param1, param2):
                return f'Request: {param1}, {param2}'

            while True:
                res = await ttr.run('PARAM_1', cb=some_request, param2='PARAM_2')
                print(res)

        except Exception as e:
            print(f"An error occurred: {e}")

        # OUT:
        # Request: PARAM_1, PARAM_2
        # Request: PARAM_1, PARAM_2
        # ...
    ```
    """
    rate_limits: List[RateLimit]
    delta_t: float

    # META
    __counter: int = 0
    __last_time_ran: float = 0
    __sliding_windows: List[_SlidingWindow]

    def __init__(self, rate_limits: List[RateLimit], delta_t: Optional[float] = None):
        """
        Args:
            rate_limits (List[RateLimit]): A list of RateLimit objects defining the rate limits.
            delta_t (Optional[float]): The time step speed (in seconds) for the sliding windows.
                Defaults to None and will be precalculated based on the slowest rate limit.
        """
        self.rate_limits = rate_limits
        self.delta_t = delta_t

        self.__sliding_windows = [_SlidingWindow(rate_limit) for rate_limit in rate_limits]

        # Determine the time step (delta_t) for the sliding windows:
        # NOTE:
        # We could choose different strategies here, such as fetching as fast as possible
        # by setting delta_t to the minimum of all rate limits.
        if self.delta_t is None:
            # try to distribute the requests evenly over the time windows
            self.delta_t = max([window.delta_t for window in self.__sliding_windows])
        logger.info(f"[.] Sliding windows delta_t: {self.delta_t}")

    async def __check_sliding_window(self, window: _SlidingWindow):
        if window.is_full():
            queue, ratelimit, ratelimit_time_window = window.queue, window.rate_limit.value, window.rate_limit.time_window
            logger.debug(f'[>] Sliding window ({ratelimit_time_window}s) size: {len(queue)}/{ratelimit}')
            while len(queue):
                oldest = queue[0]
                time_passed = time.time() - oldest
                # we can remove the oldest element(s)
                if time_passed >= ratelimit_time_window:
                    queue.popleft()
                # we exceeded the rate limit, so we have to wait
                elif window.is_full():
                    sleep_time = ratelimit_time_window - time_passed + 0.6  # 0.6 to account for some time errors
                    logger.debug(f"[>] Rate limit exceeded for sliding window ({ratelimit_time_window}s), sleeping for: {sleep_time}")
                    await asyncio.sleep(sleep_time)
                # sliding window is now within the limits
                else:
                    break
            logger.debug(f'[<] Sliding window ({ratelimit_time_window}s) size: {len(queue)}/{ratelimit}')

    async def run(
        self,
        *args,
        cb: Callable[..., Awaitable[Any] | Any],
        **kwargs
    ) -> Any:
        """
        Run the specified callback `cb` function while adhering to the rate limits. 
        It will take at least `delta_t` seconds before the callback is called.

        Any additional arguments and keyword arguments will be passed to the callback function.

        Args:
            cb (Callable[..., Awaitable[Any] | Any]): The awaitable callback function to be called.

        Returns:
            Any: The result of the callback function.
        """

        remaining_sleep = self.delta_t - (time.time() - self.__last_time_ran)
        if remaining_sleep > 0:
            logger.debug(f"[.] Early call detected, sleeping for: {remaining_sleep}")
            await asyncio.sleep(remaining_sleep)

        for window in self.__sliding_windows:
            await self.__check_sliding_window(window)

        if any(window.is_full() for window in self.__sliding_windows):
            raise RuntimeError("Unexpected full window detected; please report this incident")

        self.__counter += 1
        logger.debug(f"[+] Calling {cb.__name__}, iteration: {self.__counter}")

        res = None
        if asyncio.iscoroutinefunction(cb):
            res = await cb(*args, **kwargs)
        else:
            res = cb(*args, **kwargs)

        now = time.time()
        self.__last_time_ran = now
        for window in self.__sliding_windows:
            window.queue.append(now)

        return res


__ALL__ = ['RateLimit', 'SlidingWindowLoop']
