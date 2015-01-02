
import asyncio
import datetime
import logging

import spin.utils
import spin.protocol


class Application(object):

    def __init__(self, connections):

        self.protocols = []

        coro = spin.protocol.build_protocols(self, connections)
        asyncio.async(coro)

        self.heartbeat_task = spin.utils.call_periodically(10,
                                                           self.send_heartbeat)

    def __del__(self):
        self.close()

    def close(self):
        for p in self.protocols:
            p.close()

    def ping(self, *args, **kwargs):
        logging.debug("[{}] ping() args:{}, kwargs:{}".format(
            datetime.datetime.now(), args, kwargs)[:180])
        return {'result': 'OK', 'percent': 100}

    def heartbeat_answer_handler(self, *args, **kwargs):
        logging.info("{}.heartbeat_answer_handler() args:{}, kwargs:{}".format(
            self, args, kwargs)[:180])
        return 'DONE'

    def send_heartbeat(self):
        for p in self.protocols:
            p.remote_call(
                'ping',
                [],
                {},
                self.heartbeat_answer_handler,
                ttl=10.
            )

        return True
