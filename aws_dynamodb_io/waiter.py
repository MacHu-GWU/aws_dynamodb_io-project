# -*- coding: utf-8 -*-

import typing as T
import sys
import time
import itertools

__version__ = "0.2.1"


class Waiter:
    """
    Simple retry / polling with progressing status. Usage, it is common to check
    if a long-running job is done every X seconds and timeout in Y seconds.
    This class allow you to customize the polling interval and timeout,.

    Example:

    .. code-block:: python

        print("before waiter")

        for attempt, elapse in Waiter(
            delays=1,
            timeout=10,
            verbose=True,
        ):
            # check if should jump out of the polling loop
            if elapse >= 5:
                print("")
                break

        print("after waiter")


    :param delays: delay between each check
    :param timeout: timeout in seconds
    :param instant: if True, then the first check is instant
    :param indent: indent level for logging
    :param verbose: whether to print log

    .. versionchanged:: 0.2.1

        add instant parameter to make the first check instant
    """

    def __init__(
        self,
        delays: T.Union[int, float],
        timeout: T.Union[int, float],
        instant: bool = False,
        indent: int = 0,
        verbose: bool = True,
    ):
        self._delays = delays
        self.delays = itertools.repeat(delays)
        self.timeout = timeout
        self.instant = instant
        self.tab = " " * indent
        self.verbose = verbose

    def __iter__(self):
        k = 1
        start = time.time()
        end = start + self.timeout

        if self.instant:
            initial_attempt = 1
        else:
            initial_attempt = 0

        if self.verbose:
            sys.stdout.write(
                f"start waiter, polling every {self._delays} seconds, "
                f"timeout in {self.timeout} seconds.\n"
            )
            sys.stdout.flush()
            sys.stdout.write(
                f"\r{self.tab}on {initial_attempt} th attempt, "
                f"elapsed 0 seconds, "
                f"remain {self.timeout} seconds ..."
            )
            sys.stdout.flush()

        if self.instant:
            k += 1
            yield 1, 0

        for attempt, delay in enumerate(self.delays, k):
            now = time.time()
            remaining = end - now
            if remaining < 0:
                raise TimeoutError(f"timed out in {self.timeout} seconds!")
            else:
                time.sleep(min(delay, remaining))
                elapsed = int(now - start + delay)
                if self.verbose:  # pragma: no cover
                    sys.stdout.write(
                        f"\r{self.tab}on {attempt} th attempt, "
                        f"elapsed {elapsed} seconds, "
                        f"remain {self.timeout - elapsed} seconds ..."
                    )
                    sys.stdout.flush()
                yield attempt, int(elapsed)