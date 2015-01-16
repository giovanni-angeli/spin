spin
====


spin is a simple implementation of an rpc framework based on python3, asyncIO
and aiozmq (http://aiozmq.readthedocs.org).

spin can be used programmatically to develop a distributed system of
applications communicating with each other, but also from inside ipython3
to test/debug/verify etc. asyncio applications interactively.

see examples/examples.py for some code illustrating how to use it; e.g.
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

then in another instance of IPython3 (another console):

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

and then you have two processes exchanging data among them and you can interact
with the via its own IPython3 console.
