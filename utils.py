# coding: utf-8

import asyncio
import functools
import signal
import logging


def call_periodically(period, to_be_called, *args, on_terminate=None):

    @asyncio.coroutine
    def _call_periodically(to_be_called, *args):
        while True:
            r = to_be_called(*args)
            if not r:
                logging.warning('{} returned {}. stopping task.'.format(
                                                            to_be_called, r))
                break
            yield from asyncio.sleep(period)

    tsk = asyncio.async(_call_periodically(to_be_called, *args))

    if on_terminate is not None:
        tsk.add_done_callback(on_terminate)

    logging.info(tsk)
    return tsk


def run_loop(ttl=None, stopper_signals=(signal.SIGINT, signal.SIGTERM)):

    loop = asyncio.get_event_loop()

    if ttl:
        ch = loop.call_later(ttl, loop.stop)
        logging.warning("run_loop() ch:{}, ttl:{}".format(ch, ttl))

    def sig_handler(sig):
        logging.debug("got signal {}; exit".format(sig))
        loop.stop()

    for sig in stopper_signals:
        loop.add_signal_handler(sig,
                                functools.partial(sig_handler, sig))

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
