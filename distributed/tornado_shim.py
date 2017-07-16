"""Adapters to match the Tornado API."""
import asyncio
import math


class PeriodicCallback:
    """Schedules the given callback to be called periodically.

    The callback is called every ``callback_time`` milliseconds.
    Note that the timeout is given in milliseconds, while most other
    time-related functions in Tornado use seconds.

    If the callback runs for longer than ``callback_time`` milliseconds,
    subsequent invocations will be skipped to get back on schedule.

    `start` must be called after the `PeriodicCallback` is created.

    .. versionchanged:: 5.0
       The ``io_loop`` argument (deprecated since version 4.1) has been removed.
    """
    # taken from https://github.com/tornadoweb/tornado/blob/edf1232c3502a3755c8e87e6fa63c7c63adf9b0f/tornado/ioloop.py

    def __init__(self, callback, callback_time):
        self.callback = callback
        if callback_time <= 0:
            raise ValueError("Periodic callback must have a positive callback_time")
        self.callback_time = callback_time
        self.event_loop = asyncio.get_event_loop()
        self._running = False
        self._handle = None
        self._next_timeout = None

    def start(self):
        """Starts the timer."""
        self._running = True
        self._next_timeout = self.event_loop.time()
        self._schedule_next()

    def stop(self):
        """Stops the timer."""
        self._running = False
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None

    def is_running(self):
        """Return True if this `.PeriodicCallback` has been started.

        .. versionadded:: 4.1
        """
        return self._running

    def _run(self):
        if not self._running:
            return
        try:
            return self.callback()
        except Exception as e:
            msg = "PerioicCallback exception caught"
            self.event_loop.call_exception_handler(dict(message=msg, exception=e))
        finally:
            self._schedule_next()

    def _schedule_next(self):
        if self._running:
            current_time = self.event_loop.time()

            if self._next_timeout <= current_time:
                callback_time_sec = self.callback_time / 1000.0
                self._next_timeout += (math.floor((current_time - self._next_timeout) /
                                                  callback_time_sec) + 1) * callback_time_sec

            self._handle = self.event_loop.call_at(self._next_timeout, self._run)
