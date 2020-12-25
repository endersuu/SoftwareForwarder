import io
import json
import logging
import time
from itertools import chain
from typing import List, Tuple, Dict, Set, Callable

import boto3

import softwareforwarder

FORMAT = f"%(filename)-13s|%(funcName)-10s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context: dict):
    """
    lambda_handler() is the entry point of lambda platform
    :param event: event is a dictionary, used to receive messages from invoker
    """
    start_time = time.time()
    try:
        @softwareforwarder.SoftwareForwarder(url=event['url'])
        def main(event, context):
            sen: Callable = event['sen']
            rec: Callable = event['rec']
            uid: int = event['uid']  # uid = 2 indicates that this instance is responsible for sub-graph 2
            peers: List[int] = event['peers']

            logger.info(f'uid = {uid}')

            """
            In this simple distributed PageRank example, each instance will be assigned a uid at the beginning. By uid, 
            its corresponding sub-graph can be located. then instance is starting calculating PageRank iteratively, after
             each iteration, instances exchange information with other instances. As long as an instance receives enough 
            information, the next iteration will start.
            """
            s3 = boto3.client('s3')

            logger.info(f'''bucket = {event["bucket"]}\ndataset = {event["dataset"]}\n
            r_dataset={event["r_dataset"]}\npartition= {event["partition"]}''')

            logger.info(f'data files downloading start')

            dataset = io.BytesIO()
            s3.download_fileobj(event["bucket"], event["dataset"], dataset)

            r_dataset = io.BytesIO()
            s3.download_fileobj(event["bucket"], event["r_dataset"], r_dataset)

            partition = io.BytesIO()
            s3.download_fileobj(event["bucket"], event["partition"], partition)

            dataset = dataset.getvalue().decode()
            r_dataset = r_dataset.getvalue().decode()
            partition = tuple(map(lambda x: int(str.strip(x)), partition.getvalue().decode().split('\n')))
            turns = range(1, 1 + event['rounds'])

            logger.info('page rank initialization start')

            lines = dataset.split('\n')[:-1]
            r_lines = r_dataset.split('\n')[:-1]
            """
            v: vertex
            e: edge
            sg: sub-graph
            in: internal
            co: correlated
            """
            outgo_es = tuple(tuple(map(int, lines[i].strip().split(' ')) if lines[i] != '' else (i + 1,))
                             for i in range(len(lines)))
            logger.debug(f'outgo_es = {outgo_es}')

            in_vs = tuple(i + 1 for i in range(len(partition)) if partition[i] == uid)
            logger.debug(f'in_vs = {in_vs}')

            in_v__outgo_sgs_map = {v: tuple({partition[e - 1] for e in outgo_es[v - 1] if partition[e - 1] != uid})
                                   for v in in_vs}
            logger.debug(f'in_v__outgo_sgs_map = {in_v__outgo_sgs_map}')

            outgo_sg__in_vs_map = {
                sg: tuple(filter(lambda _in_v: sg in in_v__outgo_sgs_map[_in_v], in_v__outgo_sgs_map))
                for sg in set(chain.from_iterable(in_v__outgo_sgs_map.values()))}
            logger.debug(f'outgo_sg__in_vs_map = {outgo_sg__in_vs_map}')

            in_v__income_es_idx_map = \
                {_in_v: tuple(map(int, (('' if outgo_es[_in_v - 1] != (_in_v,) else f'{_in_v} ') + r_lines[_in_v - 1])
                                  .strip().split(' '))) for _in_v in in_vs if r_lines[_in_v - 1] != ''}
            logger.debug(f'in_v__income_es_idx_map = {in_v__income_es_idx_map}')

            co_vs = tuple(set(chain.from_iterable(in_v__income_es_idx_map.values())))
            co_v__outgo_es_num_map = {v: len(outgo_es[v - 1]) for v in co_vs}
            logger.debug(f'co_v__outgo_es_num_map = {co_v__outgo_es_num_map}')

            pagerank = {v: 1 for v in set(co_vs + in_vs)}
            # message buffer := {turn:[msg_update1,msg_update2],}
            msg_buff: Dict[int, List[Tuple]] = {}

            logger.info(f'page rank calculation start')

            for turn in turns:
                logger.info(f'uid {uid} start turn {turn}!\n' + '=' * 60)
                _pagerank = {v: 0 for v in set(co_vs + in_vs)}
                logger.debug(f'pagerank = {pagerank}')
                """Calculate pagerank"""
                for _in_v in in_v__income_es_idx_map:
                    for _co_v in in_v__income_es_idx_map[_in_v]:
                        _pagerank[_in_v] += pagerank[_co_v] / co_v__outgo_es_num_map[_co_v]

                """Send pagerank"""
                # msg_update := (turn,{vertex1:pagerank1,vertex2:pagerank2,})
                for _sg, _vs in outgo_sg__in_vs_map.items():
                    msg_update: Tuple[int, Dict] = (turn, {_v: _pagerank[_v] for _v in _vs})
                    logger.debug(f'_sg = {_sg}, msg_update = {msg_update}')
                    sen(_sg, msg_update)

                """Receive pagerank"""
                vs_wait: Set[int] = set(co_vs)
                vs_wait.difference_update(in_vs)
                while vs_wait:
                    if turn in msg_buff:  # check if any msg_update of this turn left in msg_buff
                        msg_update: Tuple[int, Dict] = msg_buff[turn].pop()
                        if not msg_buff[turn]:
                            del msg_buff[turn]
                    else:
                        msg_update: Tuple[int, Dict] = rec()
                        if msg_update[0] != turn:  # msg_update is of another turn, buffer it
                            if msg_update[0] in msg_buff:
                                msg_buff[msg_update[0]].append(msg_update)
                            else:
                                msg_buff[msg_update[0]] = [msg_update]
                            continue

                    """Update pagerank"""
                    _pagerank.update(msg_update[1])
                    vs_wait.difference_update(msg_update[1].keys())

                pagerank = _pagerank

                logger.debug(f'uid {uid} turn {turn} finish, page rank = {pagerank}')

            return uid, pagerank

        uid, result, = main(event, context)
        duration = time.time() - start_time

        # TODO maximum of return data is 6MB, it might be better to put result to S3blacks

        return {
            'uid': uid,
            'res': result,
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!'),
            'duration': duration,
        }

    except Exception as e:
        duration = time.time() - start_time
        err = str(e)
        return {
            'Exception': err,
            'statusCode': 400,
            'body': json.dumps('Something wrong from Lambda!'),
            'duration': duration,
        }
