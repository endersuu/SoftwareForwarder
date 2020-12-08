#!/usr/bin/python3
import asyncio
import logging
import time
import uuid
from typing import Optional

import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel

FORMAT = f"%(filename)-13s|%(funcName)-10s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = FastAPI()


class Data(BaseModel):
    payload: Optional[str] = None


class Peer(object):
    uid_pool = list(range(1000, 0, -1))

    def __init__(self, ip, port, uid=None):
        self.puid: str = uuid.uuid4().hex if uid is None else uid
        self.ip = ip
        self.port = port
        self.msgs_q = asyncio.Queue()

    def __str__(self):
        return f'Peer object: puid = {self.puid}, {self.ip}:{self.port}'

    __repr__ = __str__


PEERS_NUM = 10
logger.info(f'PEERS_NUM = {PEERS_NUM}')
peers = {}  # {puid:peer}


@app.get("/hello/")
async def hello():
    return {"Hello": "World"}


@app.post("/hang/")
async def hang(hang_time: int, request: Request):
    time.sleep(hang_time)
    ip = request.client.host
    port = request.client.port
    logger.debug('POS HANG:receive hang post from {}:{}'.format(ip, port))
    return {"HangTime": hang_time}


@app.get("/hang/")
async def hang(hang_time: int, request: Request):
    time.sleep(hang_time)
    ip = request.client.host
    port = request.client.port
    logger.debug('GET HANG:receive hang get from {}:{}'.format(ip, port))
    return {"HangTime": hang_time}


@app.get("/register/")
async def get_register(request: Request):
    ip = request.client.host
    port = request.client.port

    peer = Peer(ip=ip, port=port)
    logger.debug(f'GET REG: create {peer}')
    logger.debug(f'GET REG: receive register request from {ip}:{port}')
    peers.update({peer.puid: peer})

    logger.debug(f'GET REG: Peer list: {peers}')

    # length of peers will be modified http requests
    while len(peers) < PEERS_NUM:
        await asyncio.sleep(0.01)

    logger.debug(f'GET REG: Start response')
    return {'puid': peer.puid, 'peers': tuple(peers), }


@app.delete("/unregister/")
async def delete_register(src: str, request: Request):
    ip = request.client.host
    port = request.client.port

    if src in peers:
        del peers[src]
    else:
        logger.warning(f'DEL REG: {src} is not in peers')

    # connection closed by client

    response = {'type': 'uid message', 'uid': src, 'peers': tuple(peers.keys())}

    return response


@app.post("/messages/")
async def post_messages(src: str, dst: str, msg: Data, request: Request):
    ip = request.client.host
    port = request.client.port

    logger.debug(f'POS MSG:receive http post from {ip}:{port}, data = "{msg}"')

    await peers[dst].msgs_q.put(msg)

    return {'response': f'peer {src} post message {msg} to peer {dst} success'}


@app.get("/messages/")
async def read_message(src: str):
    msg = await peers[src].msgs_q.get()

    logger.debug(f'GET MSG:peer = {peers[src]}\nmessage = {msg}:{type(msg)}')

    return msg


if __name__ == '__main__':
    uvicorn.run(app, host='172.31.20.160')
