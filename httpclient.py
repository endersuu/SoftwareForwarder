import ast
import json
import multiprocessing
import typing

import softwareforwarder

import logging

FORMAT = f"%(filename)-13s|%(funcName)-10s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from enum import Enum, unique


@unique
class MsgType(Enum):
    PAGERANK_CALCULATION_DONE = 1
    PAGERANK_CALCULATION_ACK = 2
    PAGERANK_UPDATE = 3


def lambda_handler(event: dict, context: dict):
    """
    lambda_handler() is the entry point of lambda platform
    :param event: event is a dictionary, used to receive messages from invoker
    """
    @softwareforwarder.SoftwareForwarder(url=event['url'])
    def main(event, context):
        """
        main() is the function that users start coding their own stuffs. Software forwarder works in form of a decorator
        here. The main purpose is to finish register process before running user logic. After that, it will intercept
        and modify event variable to expose 2 unidirectional pipes, a uid and a peer list to the user logic
        """
        pipe_sen: multiprocessing.connection.Connection = event['pipe_sen']
        pipe_rec: multiprocessing.connection.Connection = event['pipe_rec']
        uid: int = event['uid']  # if uid = 2, then this instance is responsible for the subgraph 2
        peers: typing.List[int] = event['peers']

        logger.info(f'uid = {uid}')
        """
        In this distributed PageRank example, each instance will be assigned a uid at the beginning. By uid, its
        corresponding subgraph can be located. then instance is starting calculating PageRank iteratively, after each
        iteration, instances exchange information with each other, as long as an instance gets enough information, it is
         prepared for the next calculation iteration.
        """
        turns = range(1, 1 + event['rounds'])
        raw_data = event['raw_data']
        partition = event['partition']

        logger.info(f'raw_data = {raw_data}\npartition= {partition}')

        def append_to_pair(dictionary, key, value):
            if key in dictionary:
                dictionary[key].add(value)
            else:
                try:
                    dictionary[key] = {value}
                except TypeError as e:
                    logger.critical(
                        f'dictionary = {dictionary}\nkey = {key}, {type(key)}\nvalue = {value}, {type(value)}')

        # initialize from raw_data and partition
        lines = tuple(map(str.strip, raw_data.split('\n')))
        # {1: 3, 2: 2, 3: 2, 4: 2, 5: 3, 6: 1, 7: 1}
        vertex_subgraph_mapping: dict = dict(enumerate(partition, 1))
        logger.debug(f'vertex_subgraph_mapping = {vertex_subgraph_mapping}')

        # internal vertices (2, 3, 4)
        internal_vertices = tuple(i + 1 for i in range(len(partition)) if partition[i] == uid)
        logger.debug(f'internal_vertices = {internal_vertices}')
        edges_each_internal_vertex = dict(
            zip(internal_vertices,
                tuple(tuple(map(int, lines[vertex - 1].split(' '))) for vertex in internal_vertices)))
        # (3, 4, 4)
        edge_amount_each_internal_vertex = tuple(len(lines[vertex - 1].split(' ')) for vertex in internal_vertices)
        # {2: 3, 3: 4, 4: 4}
        internal_vertices_edge_amount_mapping = dict(zip(internal_vertices, edge_amount_each_internal_vertex))
        logger.debug(f'internal_vertices_edge_amount_mapping = {internal_vertices_edge_amount_mapping}')
        # {2: {3}, 3: {3}, 4: {1}}
        # 2: {3} means sending updated value of vertex 2 to subgraph 3
        internal_vertices_connect_subgraph_mapping = {}
        for vertex1 in internal_vertices:
            for vertex2 in lines[vertex1 - 1].split(' '):
                vertex2: int = int(vertex2)
                if vertex_subgraph_mapping[vertex2] != uid:
                    append_to_pair(internal_vertices_connect_subgraph_mapping, vertex1,
                                   vertex_subgraph_mapping[vertex2])
        logger.debug(f'internal_vertices_connect_subgraph_mapping = {internal_vertices_connect_subgraph_mapping}')
        # subgruph_connect_internal_veteices_mapping
        # {3: {2,3}, 1: {4}}

        '''connected_vertices are the vertices not internal but connecting to this subgraph'''
        connected_vertices: set = set()
        [[connected_vertices.add(int(connected_vertex)) for connected_vertex in lines[internal_vertex - 1].split(' ')
          if int(connected_vertex) not in internal_vertices] for internal_vertex in internal_vertices]
        # 1, 5, 6, 7
        connected_vertices: tuple = tuple(connected_vertices)
        # 3, 3, 3, 2
        edge_amount_each_connected_vertex = tuple(len(lines[vertex - 1].split(' ')) for vertex in connected_vertices)
        # {1: 3, 5: 3, 6: 3, 7: 2}
        connected_vertices_edge_amount_mapping = dict(zip(connected_vertices, edge_amount_each_connected_vertex))

        # [1, 1, 1, 1, 1, 1, 1] init
        internal_and_connected_vertices_pagerank = {**dict(((vertex, 1) for vertex in internal_vertices)),
                                                    **dict(((vertex, 1) for vertex in connected_vertices))}
        # {1: 3, 5: 3, 6: 3, 7: 2, 2: 3, 3: 4, 4: 4}
        internal_and_connected_vertices_edges_amount_mapping = {**connected_vertices_edge_amount_mapping,
                                                                **internal_vertices_edge_amount_mapping}
        # {1, 3}
        # connected_subgraph = set(partition[connected_vertex - 1] for connected_vertex in connected_vertices)

        # message buffer {turn:{message1,message2},}
        msg_buff: typing.Dict[int:typing.Set[dict]] = {}
        '''Start calculation iteration'''
        for turn in turns:
            logger.debug(f'DEBUG: UID {uid} Start turn {turn}!')
            internal_and_connected_vertices_pagerank_new = {**dict(((vertex, 0) for vertex in internal_vertices)),
                                                            **dict(((vertex, 0) for vertex in connected_vertices))}

            '''calculation'''
            # calculate page rank of internal vertices
            for internal_vertex in internal_vertices:
                for connected_vertex in edges_each_internal_vertex[internal_vertex]:
                    # update pagerank of internal_vertex
                    internal_and_connected_vertices_pagerank_new[internal_vertex] += \
                        internal_and_connected_vertices_pagerank[connected_vertex] / \
                        internal_and_connected_vertices_edges_amount_mapping[connected_vertex]

            '''Exchange pagerank'''
            logger.debug('send result to others')
            logger.debug(f'internal_vertices_connect_subgraph_mapping = {internal_vertices_connect_subgraph_mapping}')
            update_msgs: dict = dict()  # {subgraph:{vertex1:new_value1,vertex2:new_value2,},}
            # merge messages of each subgraph into one
            for internal_vertex in internal_vertices_connect_subgraph_mapping:
                logger.debug(
                    f"internal_vertices_connect_subgraph_mapping = {internal_vertices_connect_subgraph_mapping}")
                logger.debug(f"internal_vertex = {internal_vertex}")
                dsts = internal_vertices_connect_subgraph_mapping[internal_vertex]
                for dst_graph in dsts:
                    logger.debug(f'dst_graph = {dst_graph}')
                    msg_update_payload_component: str = str(
                        {internal_vertex: internal_and_connected_vertices_pagerank_new[internal_vertex]})
                    append_to_pair(update_msgs, dst_graph, msg_update_payload_component)
                    logger.debug(f'update_msgs = {update_msgs}')

            # send messages to each subgraph
            for msg_update_raw in update_msgs.items():
                # {'MsgType': MsgType.PAGERANK_UPDATE.value, 'MsgTurns': turn,
                # 'MsgBody': '{vertex1:new_value1,vertex2:new_value2,}'}
                msg_update = str(
                    {'MsgType': MsgType.PAGERANK_UPDATE.value, 'MsgTurns': turn, 'MsgBody': msg_update_raw[1]})
                dst_graph = msg_update_raw[0]
                logger.debug(f'send message {msg_update} to peer {dst_graph}')
                pipe_sen.send((dst_graph, msg_update))

            # now it is possible that data update messages are mixed with sync messages
            vertices_wait_for_update: typing.Set[int] = set(connected_vertices)
            logger.debug(f'DEBUG: Turn {turn}, reset vertices_wait_for_update = {vertices_wait_for_update}')
            while vertices_wait_for_update:
                # check message buffer first
                if turn in msg_buff and msg_buff[turn]:
                    msg: dict = ast.literal_eval(msg_buff[turn].pop())
                else:
                    msg: dict = ast.literal_eval(pipe_rec.recv()['payload'])
                    # message is of another iteration, pending
                    if msg['MsgTurns'] != turn:
                        # dict can not stored as value
                        append_to_pair(msg_buff, key=msg['MsgTurns'], value=str(msg))
                        continue

                logger.debug(f'DEBUG: Receive message: {msg}')

                if msg['MsgType'] == MsgType.PAGERANK_UPDATE.value:
                    for msg_update_payload_component in msg['MsgBody']:
                        msg_update_payload_component: typing.Dict[int, int] = ast.literal_eval(
                            msg_update_payload_component)
                        internal_and_connected_vertices_pagerank_new.update(msg_update_payload_component)
                        logger.debug(f'DEBUG: Turn {turn}, '
                                     f'remove {tuple(msg_update_payload_component.keys())[0]} from {vertices_wait_for_update}')
                        vertices_wait_for_update.remove(tuple(msg_update_payload_component.keys())[0])

                    logger.debug(f'DEBUG: Turn {turn}, vertices_wait_for_update = {vertices_wait_for_update}')

            # time.sleep(0.5)

            # update internal_and_connected_vertices_pagerank
            internal_and_connected_vertices_pagerank = internal_and_connected_vertices_pagerank_new.copy()
            # {2: 0.9550931383651784, 3: 1.2721304379502132, 4: 1.2726672189391413, 1: 0.95349923778112,
            #  5: 0.9555363712021046, 6: 0.9538549995658158, 7: 0.6372185961964243} (1,11)

            logger.debug(f'uid {uid} turn {turn} finish, page rank = {internal_and_connected_vertices_pagerank}')

        # FIXME variable 'turn' may not exist
        return uid, internal_and_connected_vertices_pagerank

    uid, result, = main(event, context)

    return {
        'uid': uid,
        'res': result,
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


if __name__ == '__main__':
    res: dict = lambda_handler({'event': 1}, {'context': 1})
    print(res)
