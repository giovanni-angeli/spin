# coding: utf-8

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


class PendingDialog(object):

    def __init__(self, msg, callback, ttl):
        self.t0 = time.time()
        self.msg = msg
        self.callback = callback
        self.callback_fired = False
        self.ttl = ttl

    def __str__(self):
        return 'PendingDialog[callback:{}, timed_out:{}]'.format(
            self.callback, self.timed_out())

    def timed_out(self):
        ret = time.time() > self.t0 + self.ttl
        if ret and not self.callback_fired:
            remote_id = self.msg[5]
            self.callback_fired = True
            self.callback('timed out', remote_id)
        return ret


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

        """ called by the transport level when connection is made.
        """

        self.transport = transport
        logging.debug(
            '{}.connection_made() {}, '.format(self, transport.bindings())
            + '{}, '.format(transport.connections())
        )

        self.heartbeat_task = spin.utils.call_periodically(10,
                                                       self._send_heartbeat)

    def connection_lost(self, exc):

        """ called by the transport level when connection is lost.

        NOTE: ZMQ lib already handles all of the disconnect/reconnect stuff

        """

        self.on_close.set_result(exc)
        logging.warning('{}.connection_lost()'.format(self))

    def msg_received(self, msg):

        """ called by the transport level when message is received.
        """

        msg = [m.decode() for m in msg]
        tok_id = msg[3]
        remote_id = msg[5]
        self.remote_id = remote_id
        self.parent.remote_id2protocol[remote_id] = self
        try:
            if self.pendind_tok_ids.get(tok_id):
                self._handle_remote_answer(msg)
            else:
                self._handle_remote_call(msg)
        except:
            logging.warning(traceback.format_exc())

    def _check_pending_calls(self):
        logging.debug('[{}]{}._check_pending_calls() {} '.format(
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
        expire_time = msg[4]

        if expire_time:
            d = time.time() - float(expire_time)
            if d > 0:
                raise Exception('timed out d:{}'.format(d))

        args = json.loads(args)
        kwargs = json.loads(kwargs)

        if callable:
            if callable == '__heartbeat':
                ret = {'result': 'OK'}
            elif callable.startswith('__') and callable.endswith('__'):
                ret = eval(callable.strip('__'))
            else:
                ret = eval('self.parent.%s(*args, **kwargs)' % callable)
            if ret:
                answer = [
                    ''             .encode('utf-8'),
                    json.dumps([]) .encode('utf-8'),
                    json.dumps(ret).encode('utf-8'),
                    tok_id         .encode('utf-8'),
                    ''             .encode('utf-8'),
                    self.parent.id .encode('utf-8'),
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
        p.callback(self.remote_id, **kwargs)

        if not float(pending) > 0:
            self.pendind_tok_ids.pop(tok_id)

        logging.debug("{}._handle_remote_answer() "
                        "{} pending tok ids, p:{}, msg:{}".format(
                        self, len(self.pendind_tok_ids), p, msg)[:300])

    def remote_call(self,
                    remote_callable,
                    args,
                    kwargs,
                    answer_handler,
                    ttl,
                    ):

        """ called by the application level to execute remote procedures.

        arguments:

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

        now = time.time()
        tok_id = '{:.3f}_{:03d}'.format(now % 60, random.randint(1, 1000))
        try:
            expire_time = '{:.3f}'.format(now+ttl)
            msg = [
                remote_callable   .encode('utf-8'),
                json.dumps(args)  .encode('utf-8'),
                json.dumps(kwargs).encode('utf-8'),
                tok_id            .encode('utf-8'),
                expire_time       .encode('utf-8'),
                self.parent.id    .encode('utf-8'),
            ]

            logging.debug("{}.remote_call() msg:{}".format(self, msg)[:200])

            if self.transport:
                self.transport.write(msg)
                if answer_handler:
                    p = PendingDialog(msg, answer_handler, ttl)
                    self.pendind_tok_ids[tok_id] = p
        except:
            logging.warning(traceback.format_exc())

    def close(self):
        if self.transport:
            self.transport.close()

    def _handle_heartbeat(self, *args, **kwargs):
        if args[0] == 'timed out':
            remote_id = args[1]
            if self.parent.remote_id2protocol.get(remote_id):
                self.parent.remote_id2protocol.pop(remote_id)

    def _send_heartbeat(self):
        self.remote_call(
            '__heartbeat',
            [],
            {},
            self._handle_heartbeat,
            ttl=10.,
        )
        return True


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
