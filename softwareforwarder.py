import base64
import functools
import logging
import multiprocessing
import multiprocessing.connection
import pickle
import time
from typing import Tuple, Dict

import requests

FORMAT = f"%(filename)-13s|%(funcName)-10s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SoftwareForwarder(object):
    """
    SoftwareForwarder works in form of a decorator. The main purpose of SoftwareForwarder is to let function instances
    easily and efficiently exchange states between each other.
    """

    def __init__(self, url: str = 'http://172.31.20.160:8000'):
        self.url = url
        # AWS lambda will try to reuse existed object that means code of __init__ may be skipped.
        # All the code of init has to be static value. Dynamic code has to be moved to other method.
        self.pipe_rec_r, self.pipe_rec_s = None, None
        self.pipe_sen_r, self.pipe_sen_s = None, None
        self.original_peers = None
        self.peers = None
        self.puid = None
        self.uid = None
        self.peers_original_peers_mapping = None

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            self.pipe_rec_r, self.pipe_rec_s = multiprocessing.Pipe(duplex=False)
            self.pipe_sen_r, self.pipe_sen_s = multiprocessing.Pipe(duplex=False)

            def sen(dst, msg):
                self.pipe_sen_s.send((dst, msg))

            def rec():
                return self.pipe_rec_r.recv()

            # register
            response = requests.get(self.url + '/register/')
            logger.info('register finished')
            self.original_peers: Tuple[str] = tuple(response.json()['peers'])
            self.peers = tuple(range(1, len(self.original_peers) + 1))
            self.puid: str = response.json()['puid']
            self.uid: int = self.original_peers.index(self.puid) + 1
            self.peers_original_peers_mapping: Dict[int, str] = dict(zip(self.peers, self.original_peers))

            receive_process = multiprocessing.Process(target=self.receive, args=(self.pipe_rec_s,))
            send_process = multiprocessing.Process(target=self.send, args=(self.pipe_sen_r,))
            receive_process.start()
            send_process.start()

            args[0].update({'sen': sen,
                            'rec': rec,
                            'uid': self.uid,
                            'peers': self.peers,
                            })

            logger.info(f'user function "{func.__name__}" start')
            func_return = func(*args, **kw)
            logger.info(f'user function "{func.__name__}" returned')

            # make sure pipe_sen_r is empty
            while self.pipe_sen_r.poll():
                time.sleep(0.01)

            receive_process.terminate()
            send_process.terminate()

            receive_process.join()
            send_process.join()

            receive_process.close()
            send_process.close()

            # unregister
            requests.delete(self.url + '/unregister/', params={'src': self.puid})

            return func_return

        return wrapper

    def receive(self, pipe_rec: multiprocessing.connection.Connection):
        logger.info('receive() start!')
        with requests.sessions.Session() as session:
            while True:
                # TODO This infinitely get request will make uvicorn hard to terminate
                response = session.get(self.url + '/messages/', params={'src': self.puid})
                msg: Tuple[int, Dict] = pickle.loads(base64.b64decode((response.json()['payload'].encode('utf8'))))
                logger.debug(f'receive(): client {self.puid} get msg: {msg}')
                pipe_rec.send(msg)  # already implicitly acquire mutex

    def send(self, pipe_sen: multiprocessing.connection.Connection):
        logger.info('send() start!')
        with requests.sessions.Session() as session:
            while True:
                msg_tuple: tuple = pipe_sen.recv()
                # find dst and payload from msg
                dst = self.peers_original_peers_mapping[msg_tuple[0]]
                # TODO haven't found the way to transfer binary data through fastAPI
                msg = {'payload': base64.b64encode(pickle.dumps(msg_tuple[1])).decode('utf8')}
                logger.debug(f'send(): send message to {dst}: {msg}')
                session.post(self.url + '/messages/', params={'dst': dst}, json=msg)
