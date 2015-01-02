
import os
import asyncio
import aiozmq
import zmq
import random
import json
import logging
import time
import traceback

import spin.utils


class PendingCall(object):

    def __init__(self, msg, answer_handler, ttl):
        self.t0 = time.time()
        self.msg = msg
        self.answer_handler = answer_handler
        self.ttl = ttl

    def __str__(self):
        return 'PendingCall[answer_handler:{}, timed_out:{}]'.format(
            self.answer_handler, self.timed_out())

    def timed_out(self):
        return time.time() > self.t0 + self.ttl


class Protocol(aiozmq.ZmqProtocol):

    def __init__(self, parent, on_close):
        self.on_close = on_close
        self.parent = parent
        self.parent.protocols.append(self)
        self.transport = None
        self.pendind_tok_ids = {}
        self._check_pending_calls_task = spin.utils.call_periodically(5,
                                                  self._check_pending_calls)

    def connection_made(self, transport):
        self.transport = transport
        logging.debug(
            '{}, '.format(transport.bindings())
            + '{}, '.format(transport.connections())
        )

    def connection_lost(self, exc):
        self.on_close.set_result(exc)

    def msg_received(self, msg):
        msg = [m.decode() for m in msg]
        tok_id = msg[3]
        try:
            if self.pendind_tok_ids.get(tok_id):
                self._handle_remote_answer(msg)
            else:
                self._handle_remote_call(msg)
        except:
            logging.warning(traceback.format_exc())

    def _check_pending_calls(self):
        logging.info('[{}]{}._check_pending_calls() {} '.format(
                        os.getpid(), self, len(self.pendind_tok_ids.keys())))

        for tok_ids in [k for k in self.pendind_tok_ids.keys()][:]:
            p = self.pendind_tok_ids[tok_ids]
            if p.timed_out():
                logging.warning('pending call timed out {}'.format(p))
                self.pendind_tok_ids.pop(tok_ids)

        return True

    def _handle_remote_call(self, msg):
        callable = msg[0]
        args = msg[1]
        kwargs = msg[2]
        tok_id = msg[3]
        if len(msg) > 4:
            d = time.time() - float(msg[4])
            if d > 0:
                raise Exception('timed out d:{}'.format(d))

        args = json.loads(args)
        kwargs = json.loads(kwargs)

        if callable:
            ret = getattr(self.parent, callable)(*args, **kwargs)
            if ret:
                answer = [
                    ''            .encode('utf-8'),
                    json.dumps([]).encode('utf-8'),
                    json.dumps(ret).encode('utf-8'),
                    tok_id        .encode('utf-8'),
                ]
                self.transport.write(answer)

        logging.debug("{}._handle_remote_call() msg:{}".format(
            self, msg)[:300])

    def _handle_remote_answer(self, msg):
        args = msg[1]
        kwargs = msg[2]
        tok_id = msg[3]

        args = json.loads(args)
        kwargs = json.loads(kwargs)

        pending = kwargs.get('pending', 0)

        p = self.pendind_tok_ids[tok_id]
        p.answer_handler(*args, **kwargs)

        if not float(pending) > 0:
            self.pendind_tok_ids.pop(tok_id)

        logging.debug("{}._handle_remote_answer() "
                        "{} pending tok ids, p:{}, msg:{}".format(
                        self, len(self.pendind_tok_ids), p, msg)[:300])

    def remote_call(self,
                    remote_callable,
                    args=[],
                    kwargs={},
                    answer_handler=None,
                    ttl=30.,
                    serializer=json.dumps
                    ):

        logging.debug("{}.remote_call() {}, {}, {}, {}, "
                                    "".format(self, remote_callable,
                                    args, kwargs, answer_handler)[:100])
        now = time.time()
        tok_id = '{:.3f}_{:03d}'.format(now % 60, random.randint(1, 1000))
        expire_time = '{:.3f}'.format(now+ttl)
        msg = [
            remote_callable   .encode('utf-8'),
            serializer(args)  .encode('utf-8'),
            serializer(kwargs).encode('utf-8'),
            tok_id            .encode('utf-8'),
            expire_time       .encode('utf-8'),
        ]
        try:
            if self.transport:
                self.transport.write(msg)
                if answer_handler:
                    p = PendingCall(msg, answer_handler, ttl)
                    self.pendind_tok_ids[tok_id] = p
        except:
            logging.warning(traceback.format_exc())

    def close(self):
        if self.transport:
            self.transport.close()

@asyncio.coroutine
def build_protocols(app_instance, connections, zmq_type=zmq.PAIR):

    logging.info('[{}] build_protocols() connections:{}'.format(
                                            os.getpid(), connections))

    on_closed = asyncio.Future()

    def done_callback(*args):
        logging.info('done_callback() args:{}'.format(args))

    on_closed.add_done_callback(done_callback)

    def protocolFactory():
        p = Protocol(app_instance, on_closed)
        logging.debug('p: {}'.format(p))
        return p

    for bind_or_connect, addr in connections:
        transport, _ = yield from aiozmq.create_zmq_connection(
            protocolFactory,
            zmq_type,
        )
        getattr(transport, bind_or_connect)(addr)
