import io
import os
import subprocess
import sys
import time

"""
This script is used to pre-process raw data set, 
It is requested to run on a machine pre-installed metis(gpmetis), and python3.6+

include:
1. Convert raw data set into the undirected(bidirectional?) graph: all_files_name -> tmp_undirected_graph, then send this graph to metis(gpmetis)
2. Metis partition the undirected graph and output a partition file: tmp_undirected_graph -> tmp_metis_result
3. Convert this partition file into the format of distributed page rank process: tmp_metis_result -> result_partition
4. Convert raw data set into the format of distributed page rank process: all_files_name -> result_dataset
"""

start_time = time.time()


def shell_command(command='ls'):
    process = subprocess.run([command], capture_output=True, shell=True)
    return process.stdout, process.stderr


PEERS_NUM = 10 if len(sys.argv) == 1 else int(sys.argv[1])
print(f'PEERS_NUM = {PEERS_NUM}')

delimiter = ' '
raw_files_fold = './facebook/'
# raw_files_fold = './'

tmp_undirected_graph = './tmp_metis'
tmp_metis_result = f'{tmp_undirected_graph}.part.{PEERS_NUM}'

result_dataset = './dataset'
result_partition = './partition'
# all_files = (
#     '0.edges',
#     '107.edges',
#     '348.edges',
# )

all_files_name = [path for path in os.listdir(raw_files_fold) if path.endswith('.edges')]
# all_files_name = ('soc-pokec-relationships.txt',)
# all_files_name = ('test.edges',)
print(f'input files: {all_files_name}')

"""start pre-process"""

all_raw_files_path = [os.path.join(raw_files_fold, path) for path in all_files_name]
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
tmp_matrix = [set() for _ in range(len(exist_nodes))]
# reformat raw data set to the format fit for metis
undirected_edges = set()
for line in contents:
    a, b = line.split(delimiter)
    a = node_mapping[int(a)]
    b = node_mapping[int(b)]
    matrix[a - 1].append(b)
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
print(f'command = gpmetis {tmp_undirected_graph} {PEERS_NUM}')
stdout, stderr = shell_command(f'gpmetis {tmp_undirected_graph} {PEERS_NUM}')
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
