# coding: utf-8

import logging
import json

import spin.application
import spin.protocol
import spin.utils

#~ logging.getLogger().setLevel('DEBUG')
#~ logging.getLogger().setLevel('INFO')
logging.getLogger().setLevel('WARNING')

FORMAT = "%(asctime)s:%(levelname)s:%(process)d:%(funcName)s():%(message)s"
logging.basicConfig(format=FORMAT)


"""
# start 49 processes from inside ipython3
>>> import spin.examples.example as ex
>>> c, processes = ex.start_many_process(50)

"""


def start_two():
    a = Example([('bind', 'tcp://127.0.0.1:9999')], 'a')
    b = Example([('connect', 'tcp://127.0.0.1:9999')], 'b', [10*[100*'BBBB_']])
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()
    return a, b


def start_three():
    a = Example(
        [('bind', 'tcp://127.0.0.1:9990'), ('connect', 'tcp://127.0.0.1:9991')],
        'a', 100*1000*'AAA_')
    b = Example(
        [('bind', 'tcp://127.0.0.1:9991'), ('connect', 'tcp://127.0.0.1:9992')],
        'b', 100*1000*'BBB_')
    c = Example(
        [('bind', 'tcp://127.0.0.1:9992'), ('connect', 'tcp://127.0.0.1:9990')],
        'c', 100*1000*'CCC_')
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()
    return a, b, c


def start_many_process(N):
    PORT_MIN = 8000
    PORT_MAX = 8000 + ((N-1)*N+(N-1))
    from multiprocessing import Process
    ports = []
    port = PORT_MIN
    endpoints_list = []
    for i in range(N):
        endpoints = []
        for j in range(N):
            if i > j:
                index = i*N+j
                bind_or_connect = 'bind'
            elif i == j:
                continue
            else:
                index = j*N+i
                bind_or_connect = 'connect'

            port = PORT_MIN + index % PORT_MAX
            ports.append(port)
            endpoints.append(
                #~ (bind_or_connect, 'tcp://127.0.0.1:{}'.format(port))
                (bind_or_connect, 'ipc://{}'.format(port))
            )
        print('%s, endpoints:%s'%(i, endpoints))
        endpoints_list.append(endpoints)

    processes = []
    for i in range(1, N):
        endpoints = endpoints_list[i]
        id = '{:03d}_'.format(i)
        p = Process(
            target=start_and_run,
            args=(endpoints, id, 10*id)
        )
        p.start()
        print(p)
        processes.append(p)

    a = Example(endpoints_list[0], '000', 10*'00_'  )
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()

    return a, processes


class Example(spin.application.Application):

    def __init__(self, connections, id, data=[10*[100*'AAAA_']]):
        super().__init__(connections, id)
        self.data = data
        self.send_data_task = spin.utils.call_periodically(10,
                                                         self.send_data)


    def send_data(self):

        args = []
        kwargs = {'data': self.data}

        def answer_handler(*args, **kwargs):
            logging.info("{} args({}):{}, kwargs({}):{}".format(
                        self.id, len(args), args, len(kwargs), kwargs)[:200])

        for id in self.remote_id2protocol.keys():
            self.remote_call(
                id,
                'data_handler',
                args,
                kwargs,
                answer_handler,
                ttl=20.,
            )

        return True

    def data_handler(self, *args, **kwargs):

        data = kwargs.get('data')
        if data:
            data = str(kwargs.get('data'))
        logging.info("{} data({}):{}".format(
                                            self.id, len(data), data)[:200])

        return {'result': 'OK', 'len_of_data': len(data)}


def start_and_run(connections=[('bind', 'tcp://127.0.0.1:9999')], id='a', data=[]):

    logging.warning('start_and_run() connections:{}'.format(connections))

    a = Example(connections, id, data)
    spin.utils.run_loop()
    return a

if __name__ == '__main__':
    start_and_run()
