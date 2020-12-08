import ast
import sys
from multiprocessing.pool import ThreadPool
import botocore.session
import json
import time

session = botocore.session.get_session()
region_name = 'us-east-1'
lambda_function_name = 'client'
lambda_client = session.create_client('lambda', region_name=region_name)

"""
This is the script to invoke many lambda function instances, each instance just is responsible for a part of graph.
So I aggregate the return results for convenience 






here is a example of what raw data and related partition looks like
raw_data = r'''5 3 2
1 3 4
5 4 2 1
2 3 6 7
1 3 6
5 4 7 8
6 4 8
6 7'''

partition = (3, 2, 2, 2, 3, 1, 1, 1,)
"""

# PEERS_NUM is the number of lambda function instances, it must be same as the number of subgraph
PEERS_NUM = 10 if len(sys.argv) == 1 else int(sys.argv[1])
print(f'PEERS_NUM = {PEERS_NUM}')
url = 'http://172.31.20.160:8000'
raw_data_path = 'dataset'
partition_path = 'partition'
# number of iterations
rounds = 10

with open(raw_data_path, 'r') as raw_data_file:
    raw_data = raw_data_file.read()

with open(partition_path, 'r') as partition_file:
    partition = tuple(map(lambda line: int(line.strip()), partition_file.readlines()))

payload = {
    "raw_data": raw_data,
    "partition": partition,
    "url": url,
    "rounds": rounds,
}


def invoke(lambda_function_name, payload):
    return lambda_client.invoke(FunctionName=lambda_function_name, Payload=json.dumps(payload),
                                # InvocationType='Event'
                                )


pool = ThreadPool(PEERS_NUM)

start_time = time.time()
res = [pool.apply_async(invoke, (lambda_function_name, payload)) for i in range(1, PEERS_NUM + 1)]

# get page rank results
key_values = {}
for re in res:
    key_values.update(ast.literal_eval(re.get()['Payload'].read().decode())['res'])

print(f'time cost = {time.time() - start_time}')

values = key_values.values()
print(f'sum = {sum(values)}, max = {max(values)}, min = {min(values)}')

# print(key_values)
