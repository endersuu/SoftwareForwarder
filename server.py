import asyncio
import logging
import uuid
from typing import Optional

import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel

FORMAT = f"%(filename)-13s|%(funcName)-10s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PEERS_NUM = 80
logger.info(f'PEERS_NUM = {PEERS_NUM}')
peers = {}  # {puid:peer}

app = FastAPI()


class Data(BaseModel):
    payload: Optional[bytes] = None


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


@app.get("/register/")
async def get_register(request: Request):
    ip = request.client.host
    port = request.client.port

    peer = Peer(ip=ip, port=port)
    logger.debug(f'receive register request from {ip}:{port}, create {peer}')
    peers.update({peer.puid: peer})
    logger.debug(len(peers))
    # length of peers will be modified by the others
    while len(peers) < PEERS_NUM:
        await asyncio.sleep(0.01)

    return {'puid': peer.puid, 'peers': tuple(peers), }


@app.delete("/unregister/")
async def delete_register(src: str):
    if src in peers:
        del peers[src]
    else:
        logger.warning(f'{src} is not in peers list')
    return src


@app.post("/messages/")
async def post_messages(dst: str, msg: Data):
    await peers[dst].msgs_q.put(msg)
    logger.debug(f'dst = {dst}, data = {msg}')
    return msg


@app.get("/messages/")
async def read_message(src: str):
    msg = await peers[src].msgs_q.get()
    logger.debug(f'src = {src}, data = {msg}')
    return msg


if __name__ == '__main__':
    uvicorn.run(app, host='172.31.20.160')
