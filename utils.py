# coding: utf-8

import asyncio
import functools
import signal
import logging


def call_periodically(period, callable, *args, on_terminate=None):

    @asyncio.coroutine
    def _call_periodically(callable, *args):
        while True:
            r = callable(*args)
            if not r:
                logging.warning('{} returned {}. stopping task.'.format(
                                                            callable, r))
                break
            yield from asyncio.sleep(period)

    tsk = asyncio.async(_call_periodically(callable, *args))

    if on_terminate:
        tsk.add_done_callback(on_terminate)

    logging.info(tsk)
    return tsk


def run_loop():

    loop = asyncio.get_event_loop()

    def ask_exit(sig):
        logging.debug("got signal {}; exit".format(sig))
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig,
                                functools.partial(ask_exit, sig))

    loop.run_forever()


def set_asyncio_loop_in_inputhook(clock_resolution=None):

    loop = asyncio.get_event_loop()
    if clock_resolution:
        loop._clock_resolution = clock_resolution

    def hook():
        loop.call_soon(lambda: None)
        while loop._ready:
            loop._run_once()
        return 0

    from IPython.lib.inputhook import inputhook_manager
    inputhook_manager.set_inputhook(hook)
    loop._running = True



