# coding: utf-8

import logging
import pprint

import spin.application
import spin.protocol
import spin.utils

FORMAT = "%(asctime)s:%(levelname)s:%(process)d:%(funcName)s():%(message)s"
logging.basicConfig(format=FORMAT)


""" an example of a minmal application and of how to run it.

    you can run, in one instance of IPython3:

        >>> import spin.examples.example as ex
        >>> e = ex.Example([('bind', 'tcp://127.0.0.1:9999')], 'example_000')
        >>> import spin
        >>> spin.utils.set_asyncio_loop_in_inputhook()
        >>> import logging
        >>> logging.getLogger().setLevel('INFO')
        ...
        2015-01-16 17:47:23,628:INFO:4452:send_data():example_000.<spin.examples.example.Example object at 0xb5ebc3cc> dict_keys(['example_001'])
        2015-01-16 17:47:23,731:INFO:4452:answer_handler():example_000 args(1):('example_001',), kwargs(2):{'result': 'OK', 'len_of_data': 60}
        ...

    whuile, in another instance of IPython3 (another console):

        >>> import spin.examples.example as ex
        >>> e = ex.Example([('connect', 'tcp://127.0.0.1:9999')], 'example_001')
        >>> import spin
        >>> spin.utils.set_asyncio_loop_in_inputhook()
        >>> import logging
        >>> logging.getLogger().setLevel('INFO')
        ...
        2015-01-16 17:47:26,485:INFO:4475:send_data():example_001.<spin.examples.example.Example object at 0xb5efb66c> dict_keys(['example_000'])
        2015-01-16 17:47:26,589:INFO:4475:answer_handler():example_001 args(1):('example_000',), kwargs(2):{'len_of_data': 60, 'result': 'OK'}
        2015-01-16 17:47:28,701:INFO:4475:data_handler():example_001 data(60):['**', '**', '**', '**', '**', '**', '**', '**', '**', '**']
        ...

    and then you have two processes exchanging data between them while you can
    interact with each one via the IPython3 console.

    More to read in the see the doc strings of *start_two()*,
    *start_three()*, *start_many_processes()*.

"""


class Example(spin.application.Application):

    """ a minimal example of an application.


    It 'produces' mock data and sends them to all of the applications
    connected to it, every 5 seconds. It implements also a handler to
    receive data sent from peers.

    """

    def __init__(self, endpoints=[('bind', 'tcp://127.0.0.1:9999')],
                                        id='example_001', data=10*['**',]):

        """
        Keyword arguments:
        endpoints -- a list of couples of strings.
            each couple contains:
                a first item that can be 'bind' or 'connect',
                a second item specifying a zmq protocol+addr,
                    i.e.: 'tcp://127.0.0.1:9999'.
        id -- (str) the identifier of the instance. It is used to address
            remote calls. Each instance must have a unique id in the system.
        data -- (a jsonifyable sequnce) the data that this instance will send
            remotely.

        """

        super().__init__(endpoints, id)
        self.data = data
        self.send_data_task = spin.utils.call_periodically(5,
                                                         self.send_data)

    def send_data(self):

        args = []
        kwargs = {'data': self.data}

        logging.info("{}.{} {}".format(self.id, self,
                                            self.remote_id2protocol.keys()))

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
                ttl=5.,
            )

        return True

    def data_handler(self, *args, **kwargs):

        data = kwargs.get('data')
        if data:
            data = str(kwargs.get('data'))
        logging.info("{} data({}):{}".format(
                                            self.id, len(data), data)[:200])

        return {'result': 'OK', 'len_of_data': len(data)}


def start_and_run(endpoints=[('bind', 'tcp://127.0.0.1:9999')],
                                                    id='a', data=[], ttl=None):

    """ Create an instance of Example calss and start the asyncio loop.

        Keyword arguments:
        endpoints -- a list of couples of strings.
            each couple has:
                a first item that can be 'bind' or 'connect'
                a second item tha specify a zmq protocol,
                    i.e. 'tcp://127.0.0.1:9999'
        id -- (str) the identifier of the instance. It is used to address
            remote calls. each instance must have a unique id n the system.
        data -- (a jsonifyable sequnce) the data that this instance will send
            remotely.
        ttl -- (float or None) the time to live of this process, None
            means never.

    It can be used to run different instances in different processes setting
    the argument endpoints in a suitable way.
    """

    logging.info('start_and_run() endpoints:{}, id:{}, ttl:{}'.format(
                    endpoints, id, ttl))

    a = Example(endpoints, id, data)

    spin.utils.run_loop(ttl)
    return a


def start_two():

    """ creates two instance of Example calss inside IPython3

    This is to be called from inside an IPython3 instance; Here, two instance
    of the Example calss are created inside the same calling process, with a
    zmq connection between them. Then the asyncio loop is embedded in Ipythn3
    main loop.

    After this it is possible to interact, via console, with the running
    instances.

    """

    a = Example([('bind', 'tcp://127.0.0.1:9999')], 'a')
    b = Example([('connect', 'tcp://127.0.0.1:9999')], 'b', [10*[100*'BBBB_']])
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()
    return a, b


def start_three():

    """ creates three instance of Example calss inside IPython3

    This is to be called from inside an IPython3 instance; In the same way as
    for the *start_two()* function above, here we have three instances
    fully meshed, inside the same process embedded inside IPython.

    After this it is possible to interact, via console, with the running
    instances.

    """

    a = Example(
        [('bind', 'tcp://127.0.0.1:9990'),
                                    ('connect', 'tcp://127.0.0.1:9991')],
        'a', 100*1000*'AAA_')
    b = Example(
        [('bind', 'tcp://127.0.0.1:9991'),
                                    ('connect', 'tcp://127.0.0.1:9992')],
        'b', 100*1000*'BBB_')
    c = Example(
        [('bind', 'tcp://127.0.0.1:9992'),
                                    ('connect', 'tcp://127.0.0.1:9990')],
        'c', 100*1000*'CCC_')
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()
    return a, b, c


def start_many_processes(N, port_start=20000):

    """ Create 1 Example's instance inside IPython3 and N-1 external porcesses.


    Keyword arguments:
        N -- the total # of instances or processes.
        port_start -- a number to be used a a start point to deine the tcp/ip
            port numbers to be used. N(N-1)/2 ports will be used, in the range
            port_start, port_start+N(N-1)/2

    This is to be called from inside an IPython3 instance; It creates an
    Example's instance inside the calling process (IPython) and spawn N-1 more
    precesses instantiating one Example's instance each.
    All the inastances are connected to each others and send and receive data.

    After this it is possible to interact, via console, directly with the
    instance running inside IPython, and thrgough it with the remote instances
    too.

    """


    port_max = port_start + ((N-1)*N+(N-1))
    ports = []
    port = port_start
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

            port = port_start + index % port_max
            ports.append(port)
            endpoints.append(
                (bind_or_connect, 'tcp://127.0.0.1:{}'.format(port))
                #~ (bind_or_connect, 'ipc://{}'.format(port))
            )
        #~ logging.warning('{}, endpoints:{}'.format(i, endpoints))
        pprint.pprint('{}, endpoints:'.format(i))
        pprint.pprint(endpoints)
        endpoints_list.append(endpoints)

    from multiprocessing import Process
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

    a = Example(endpoints_list[0], '000', 10*'00_')
    import spin.utils
    spin.utils.set_asyncio_loop_in_inputhook()

    return a, processes


