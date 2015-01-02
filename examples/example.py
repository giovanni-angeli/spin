
import logging
import json

import spin.application
import spin.protocol
import spin.utils

#~ logging.getLogger().setLevel('DEBUG')
#~ logging.getLogger().setLevel('INFO')
logging.getLogger().setLevel('WARNING')


"""
# start 49 processes from inside ipython3
>>> import spin.examples.example as ex
>>> c, processes = ex.start_many_process(50)

"""


def start_two():
    a = Example([('bind', 'tcp://127.0.0.1:9999')])
    b = Example([('connect', 'tcp://127.0.0.1:9999')], [10*[100*'BBBB_']])
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()
    return a, b


def start_three():
    a = Example([('bind', 'tcp://127.0.0.1:9990'), ('connect', 'tcp://127.0.0.1:9991')], 100*1000*'AAA_')
    b = Example([('bind', 'tcp://127.0.0.1:9991'), ('connect', 'tcp://127.0.0.1:9992')], 100*1000*'BBB_')
    c = Example([('bind', 'tcp://127.0.0.1:9992'), ('connect', 'tcp://127.0.0.1:9990')], 100*1000*'CCC_')
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()
    return a, b, c


def start_many_process(N):
    BASE_PORT = 8000
    from multiprocessing import Process
    ports = []
    port = BASE_PORT
    conns_list = []
    for i in range(N):
        conns = []
        for j in range(N):
            if i > j:
                index = i*N+j
                bind_or_connect = 'bind'
            elif i == j:
                continue
            else:
                index = j*N+i
                bind_or_connect = 'connect'

            port = BASE_PORT+index%((N-1)*N+(N-1))
            ports.append(port)
            conns.append(
                (bind_or_connect, 'tcp://127.0.0.1:{}'.format(port))
            )
        print('%s, conns:%s'%(i, conns))
        conns_list.append(conns)

    processes = []
    for i in range(1, N):
        conns = conns_list[i]
        p = Process(
            target=start_and_run,
            args=(conns, 10*'{:03d}_'.format(i))
        )
        p.start()
        print(p)
        processes.append(p)

    a = Example(conns_list[0], 10*'CC_'  )
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()

    return a, processes


class Example(spin.application.Application):

    def __init__(self, connections, data=[10*[100*'AAAA_']]):
        super().__init__(connections)
        self.data = data
        self.send_data_task = spin.utils.call_periodically(10,
                                                         self.send_data)

    def answer_handler(self, *args, **kwargs):
        logging.info("{}.answer_handler() "
                            "args({}):{}, kwargs({}):{}".format(
                            self, len(args), args, len(args), kwargs)[:200])

    def send_data(self, delay=-1):

        args = json.dumps([])
        kwargs = json.dumps({'data': self.data})

        for p in self.protocols:
            p.remote_call(
                'data_handler',
                args,
                kwargs,
                self.answer_handler,
                ttl=60,
                serializer=lambda x:x
            )

        return True

    def data_handler(self, *args, **kwargs):

        data = str(kwargs.get('data'))
        logging.info("data_handler() data({}):{}".format(
                                                    len(data), data)[:200])

        return {'result': 'OK'}


def start_and_run(connections=[('bind', 'tcp://127.0.0.1:9999')], data=[]):

    logging.warning('start_and_run() connections:{}'.format(connections))

    a = Example(connections, data)
    spin.utils.run_loop()
    return a

if __name__ == '__main__':
    start_and_run()
