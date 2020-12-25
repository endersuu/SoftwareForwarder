import ast
import json
import sys
import time
from multiprocessing.pool import ThreadPool

import botocore.config
import botocore.session

session = botocore.session.get_session()
region_name = 'us-east-1'
lambda_function_name = 'client'
cfg = botocore.config.Config(retries={'max_attempts': 0}, connect_timeout=300, read_timeout=300)
lambda_client = session.create_client('lambda', region_name=region_name, config=cfg)

"""
This is the script to invoke many lambda function instances, each instance just is responsible for a part of graph.
So I aggregate the return results for convenience 

here is a example of what dataset and related partition looks like
dataset = r'''2
4
5 4 2 1 6
6 7
1

4 8
6
'''

partition = r'''1
2
2
2
1
3
3
3'''
"""

# PEERS_NUM is the number of lambda function instances, it must be same as the number of sub-graph
PEERS_NUM = 100 if len(sys.argv) == 1 else int(sys.argv[1])
print(f'PEERS_NUM = {PEERS_NUM}')
url = 'http://172.31.20.160:8000'
dataset = 'soc-pokec_dataset'
# dataset = 'test1_dataset'
r_dataset = f'r_{dataset}'
partition = f'{dataset}_partition_{PEERS_NUM}'
bucket = 'pagerankdataset'
# number of iterations
rounds = 1

event = {
    "bucket": bucket,
    "dataset": dataset,
    "r_dataset": r_dataset,
    "partition": partition,
    "url": url,
    "rounds": rounds,
}


def invoke(function_name, payload):
    return lambda_client.invoke(FunctionName=function_name, Payload=json.dumps(payload),
                                # InvocationType='Event'
                                )


pool = ThreadPool(PEERS_NUM)

start_time = time.time()
res = [pool.apply_async(invoke, (lambda_function_name, event)) for i in range(1, PEERS_NUM + 1)]

# get page rank results
key_values = {}
for re in res:
    try:
        # maximum allowed payload size is 6291556 bytes
        r = re.get()['Payload'].read().decode()
        key_values.update(ast.literal_eval(r)['res'])
    except KeyError as e:
        print(e)
        print(r)

print(f'time cost = {time.time() - start_time}')
try:
    values = key_values.values()
    print(f'sum = {sum(values)}, max = {max(values)}, min = {min(values)}')
except Exception as e:
    print(e)
    print(values)

# print(key_values)
