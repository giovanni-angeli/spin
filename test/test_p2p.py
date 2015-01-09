# coding: utf-8

from spin.examples.example import start_and_run

import logging
logging.getLogger().setLevel('INFO')

def test_p2p():

    from multiprocessing import Process

    endpoints=[('connect', 'tcp://127.0.0.1:9999')]
    id='a'
    p0 = Process(
        target=start_and_run,
        args=(endpoints, id, 10*id, 15.)
    )
    p0.start()

    endpoints=[('bind', 'tcp://127.0.0.1:9999')]
    id='b'
    p1 = Process(
        target=start_and_run,
        args=(endpoints, id, 10*id, 15.)
    )
    p1.start()

    p0.join()
    p1.join()
