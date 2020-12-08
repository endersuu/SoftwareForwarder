import typing
import functools
import time
import multiprocessing
import multiprocessing.connection

import requests

import logging

FORMAT = f"%(filename)-13s|%(funcName)-10s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SoftwareForwarder(object):
    def __init__(self, url: str = 'http://172.31.20.160:8000'):
        self.url = url
        self.pipe_rec_r, self.pipe_rec_s = None, None
        self.pipe_sen_r, self.pipe_sen_s = None, None

        """
        AWS lambda will try to reuse existed object that means code of __init__ may be skipped.
        So, all the code of init has to be static value. Dynamic code has to be moved to other method. 
        """
        self.orignal_peers = None
        self.peers = None
        self.puid = None
        self.uid = None
        self.peers_orignal_peers_mapping = None

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            logger.info(f'-> decorator "{func.__name__}" start')

            self.pipe_rec_r, self.pipe_rec_s = multiprocessing.Pipe(duplex=False)
            self.pipe_sen_r, self.pipe_sen_s = multiprocessing.Pipe(duplex=False)
            # register
            logger.debug('Before register request')
            response = requests.get(self.url + '/register/')
            logger.debug('After register request')
            self.orignal_peers: typing.Tuple[str] = tuple(response.json()['peers'])
            self.peers = tuple(range(1, len(self.orignal_peers) + 1))
            # self.peers = tuple(range(len(self.orignal_peers)))
            self.puid: str = response.json()['puid']
            self.uid: int = self.orignal_peers.index(self.puid) + 1
            self.peers_orignal_peers_mapping: typing.Dict[int, str] = dict(zip(self.peers, self.orignal_peers))
            logger.debug(f'INIT: orignal_peers = {self.orignal_peers}\n'
                         f'peers = {self.peers}\n'
                         f'puid = {self.puid}\n'
                         f'uid = {self.uid}\n'
                         f'peers_orignal_peers_mapping = {self.peers_orignal_peers_mapping}')

            logger.debug(
                f'get response from server {response}\norignal_peers = {self.orignal_peers}\npuid = {self.puid}')

            # TODO I prefer to use threads here, but I haven't find the way to terminate threads in python
            receive_process: multiprocessing.Process = multiprocessing.Process(target=self.receive,
                                                                               args=(self.pipe_rec_s,))
            send_process: multiprocessing.Process = multiprocessing.Process(target=self.send, args=(self.pipe_sen_r,))
            receive_process.start()
            send_process.start()

            # update event variable
            args[0].update({'pipe_rec': self.pipe_rec_r,
                            'pipe_sen': self.pipe_sen_s,
                            'uid': self.uid,
                            'peers': self.peers,
                            })

            logger.debug('args = {}'.format(args))
            time.sleep(1)

            logger.info(f'-> function "{func.__name__}" start')
            # execute user function
            func_return = func(*args, **kw)

            logger.info(f'-> after function "{func.__name__}" return')
            time.sleep(1)
            '''
            check if msg_q_sen is empty
            server is considered always up, so sending message is feasibly
            and here will be no more msg produced by user process because it has already returned
            '''
            logger.debug(f'self.pipe_sen_r.poll() = {self.pipe_sen_r.poll()}')
            while self.pipe_sen_r.poll():
                time.sleep(0.01)

            receive_process.terminate()
            send_process.terminate()

            receive_process.join()
            send_process.join()

            # test if process closed
            receive_process.close()
            send_process.close()

            # unregister
            requests.delete(self.url + '/unregister/', params={'src': self.puid})

            return func_return

        return wrapper

    def receive(self, pipe_rec: multiprocessing.connection.Connection):
        logger.debug(f'receive thread start!')
        while True:
            # TODO This infinitely get request will make uvicorn hard to terminate the connection.
            response = requests.get(self.url + '/messages/', params={'src': self.puid})
            logger.debug(f'receive():message = {response.json()}:{type(response.json())}')
            msg = response.json()
            logger.debug(f'receive(): client {self.puid} get msg: {msg}')

            # already implicitly acquire mutex
            pipe_rec.send(msg)

    def send(self, pipe_sen: multiprocessing.connection.Connection):
        logger.debug(f'send(): send thread start!')
        while True:
            logger.debug(f'send(): get message from msg_q_sen, message remain? {self.pipe_sen_r.poll()}')
            msg_tuple: tuple = pipe_sen.recv()
            # find dst and payload from msg
            dst = self.peers_orignal_peers_mapping[msg_tuple[0]]
            msg = {'payload': msg_tuple[1]}
            logger.debug(f'send(): send message to {dst}: {msg}')
            requests.post(self.url + '/messages/', params={'src': self.puid, 'dst': dst}, json=msg)
