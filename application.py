# coding: utf-8

import asyncio
import json

import spin.utils
import spin.protocol


class Application(object):

    def __init__(self, endpoints, id):

        self.id = id
        self.protocols = []
        self.remote_id2protocol = {}

        coro = spin.protocol.build_protocols(self, endpoints)
        asyncio.async(coro)

    def __del__(self):
        self.close()

    def close(self):
        for p in self.protocols:
            p.close()

    def remote_call(self,
                    remote_id,
                    remote_callable,
                    args=[],
                    kwargs={},
                    answer_handler=None,
                    ttl=30.,
                    ):

        """ called to execute remote procedures.

        arguments:

        remote_id -- (str) id of the remote component to call the procedure on.

        remote_callable -- (str) the method to be called on remote
            component.

        args -- (a jsonifyable list of objects) the args to be passed to
            the remote method as arglist.

        kwargs -- (a jsonifyable dict of objects) the args to be passed to
            the remote method as keyword args.

        answer_handler -- (a callable object) the callback, i.e. the function
            to be called back when an answer to this call will be received. it
            may be None

        ttl -- time to live i.e. how long to wait for an answer to this call.
            After ttl seconds since the call, if no answer has yet been
            received, it is descarded emitting a warning.
            Not used if answer_handler is None.

        """


        protocol = self.remote_id2protocol[remote_id]
        protocol.remote_call(
            remote_callable,
            args,
            kwargs,
            answer_handler,
            ttl,
        )
