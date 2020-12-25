import io
import logging
import os
import subprocess
import sys
import time

import boto3
from botocore.exceptions import ClientError

FORMAT = f"%(filename)-13s|%(funcName)-10s|%(lineno)-3d|%(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
This script is used to preprocess raw data set, modify it if it can not parse raw data set correctly. 
It is requested to run on a machine pre-installed metis(gpmetis), and python3.6+
"""

start_time = time.time()


def shell_command(command: str = 'ls'):
    process = subprocess.run([command], capture_output=True, shell=True)
    return process.stdout, process.stderr


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True


PEERS_NUM = 20 if len(sys.argv) == 1 else int(sys.argv[1])
print(f'PEERS_NUM = {PEERS_NUM}')

"""input"""
# delimiter = ' '
delimiter = '\t'
# input_files_fold = './facebook/'
input_files_fold = './'
# all_input_files_name = [path for path in os.listdir(input_files_fold) if path.endswith('.edges')]
all_input_files_name = ('soc-pokec-relationships.txt',)
print(f'input files: {all_input_files_name}')

"""output"""
tmp_undirected_graph = './tmp_metis'
tmp_metis_result = f'{tmp_undirected_graph}.part.{PEERS_NUM}'
result_dataset = 'soc-pokec_dataset'
result_r_dataset = f'r_{result_dataset}'
result_partition = f'{result_dataset}_partition_{PEERS_NUM}'

"""upload"""
bucket = 'pagerankdataset'
s3_key_dataset = result_dataset
s3_key_r_dataset = result_r_dataset
s3_key_partition = result_partition

"""start pre-process"""

all_raw_files_path = [os.path.join(input_files_fold, path) for path in all_input_files_name]
if os.path.isfile(tmp_metis_result):
    os.remove(tmp_metis_result)
    print(f'remove old "{tmp_metis_result}"')
if os.path.isfile(tmp_undirected_graph):
    os.remove(tmp_undirected_graph)
    print(f'remove old "{tmp_undirected_graph}"')

# aggregate all the contents from all raw files
contents = []
for file_path in all_raw_files_path:
    with open(file_path, 'r') as raw_file:
        contents += raw_file.readlines()

# find all the nodes
exist_nodes = set()
try:
    for line in contents:
        a, b = line.split(delimiter)
        exist_nodes.add(int(a))
        exist_nodes.add(int(b))
except Exception as e:
    print(line)
    raise e

# reassign continuous index for each node
node_mapping = dict(zip(exist_nodes, range(1, len(exist_nodes) + 1)))

matrix = [[] for _ in range(len(exist_nodes))]
r_matrix = [[] for _ in range(len(exist_nodes))]
tmp_matrix = [set() for _ in range(len(exist_nodes))]
# reformat raw data set to the format fit for metis
undirected_edges = set()
for line in contents:
    a, b = line.split(delimiter)
    a = node_mapping[int(a)]
    b = node_mapping[int(b)]
    matrix[a - 1].append(b)
    r_matrix[b - 1].append(a)
    # make relation undirected
    tmp_matrix[a - 1].add(b)
    undirected_edges.add((a, b))
    tmp_matrix[b - 1].add(a)
    undirected_edges.add((b, a))

# to fit input of distributed page rank
buffer = io.StringIO()
for edges in matrix:
    buffer.write(' '.join(map(str, edges)))
    buffer.write('\n')

with open(result_dataset, 'w') as data_set:
    data_set.write(buffer.getvalue())
buffer.close()

buffer = io.StringIO()
for edges in r_matrix:
    buffer.write(' '.join(map(str, edges)))
    buffer.write('\n')

with open(result_r_dataset, 'w') as data_set:
    data_set.write(buffer.getvalue())
buffer.close()

# to fit input format of metis
buffer = io.StringIO()
buffer.write(f'{len(exist_nodes)} {len(undirected_edges) // 2}\n')
for edges in tmp_matrix:
    buffer.write(' '.join(map(str, edges)))
    buffer.write('\n')

with open(tmp_undirected_graph, 'w') as data_set_for_metis:
    data_set_for_metis.write(buffer.getvalue())
buffer.close()

# call metis to partition graph
stdout, stderr = shell_command('sudo find  /  -iname gpmetis')
metis_path = stdout.decode().strip()
print(f'{metis_path} {tmp_undirected_graph} {PEERS_NUM}')
stdout, stderr = shell_command(f'{metis_path} {tmp_undirected_graph} {PEERS_NUM}')

if stderr:
    for line in stderr.decode().split('\n'):
        print(line)
    exit(-1)

for line in stdout.decode().split('\n'):
    print(line)

print(f'wait for {tmp_metis_result}')
while not os.path.isfile(tmp_metis_result):
    time.sleep(1)
print(f'{tmp_metis_result} is ready')

# reformat partition file to the format fit for distributed page rank, reassign subgruph index 0 to (largest index + 1)
with open(tmp_metis_result, 'r') as metis_partition_file:
    partitions = tuple(int(partition) for partition in metis_partition_file.readlines())
    max_subgraph = max(partitions) + 1
    partitions = (str(partition if partition != 0 else max_subgraph) for partition in partitions)

buffer = '\n'.join(partitions)
with open(result_partition, 'w') as result_partition_file:
    result_partition_file.write(buffer)
print(f'time consumption {time.time() - start_time}s')

# upload files to S3
upload_file(result_dataset, bucket, s3_key_dataset)
print(f'upload {result_dataset} to S3 bucket {bucket}, key = {s3_key_dataset}')
upload_file(result_r_dataset, bucket, s3_key_r_dataset)
print(f'upload {result_r_dataset} to S3 bucket {bucket}, key = {s3_key_r_dataset}')
upload_file(result_partition, bucket, s3_key_partition)
print(f'upload {result_partition} to S3 bucket {bucket}, key = {s3_key_partition}')
