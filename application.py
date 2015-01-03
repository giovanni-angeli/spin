# coding: utf-8

import asyncio
import datetime
import logging
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
                    serializer=json.dumps,
                    ):

        protocol = self.remote_id2protocol[remote_id]
        protocol.remote_call(
            remote_callable,
            args,
            kwargs,
            answer_handler,
            ttl,
            serializer,
        )
