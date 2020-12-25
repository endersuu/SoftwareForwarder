# Introduction

This project implements a middleware working as a switch in AWS Lambda platform. Serverless function instances simply
send messages with the receiver's id to an input interface, then the receiver, another serverless function instance,
will receive messages automatically.

## Software-Forwarder

### Example

```python
import softwareforwarder


def lambda_handler(event, context):
    @softwareforwarder.SoftwareForwarder(url=event['server_url'])
    def user_function(event, context):
        send: Callable = event['sen']
        receive: Callable = event['rec']
        uid: int = event['uid']
        peers: List[int] = event['peers']
        # send message to other instances
        message: Picklable = ...
        peer_id: int = ...
        send(peer_id, message)
        # receive message
        message = receive()
        ...

    user_function(event, context)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
    }

```

### High view of Software-Forwarder

![SoftwareForwarder](https://drive.google.com/uc?export=view&id=19hdtTwg1Wg0nT_fgXAdEqO2aLYnWqv9y)

## Simple Distributed PageRank

A task of simple distributed Pagerank is deployed as a demonstration of this project.
![Simple Distributed Algorithm](https://drive.google.com/uc?export=view&id=1kBq9yGylvQa4DRD2SoQneW79iH4VA_s6)

First, Partition an immense graph into small sub-tasks by
leveraging [metis](http://glaros.dtc.umn.edu/gkhome/metis/metis/overview). Then sub-tasks are sent to different workers,
each worker takes the responsibility for its sub-task. During the Pagerank calculation, necessary states will be
exchanged among workers in each iteration

### What states of Pagerank need to be exchanged?

If we look at Pagerank equation (Ignore damping here, to make Pagerank calculation simpler)  
![equation](https://latex.codecogs.com/gif.latex?P_{n&plus;1}(v_{m})=&space;\frac{P_{n}(v_{co})}{outdegree(v_{co})})  
![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{m})) is Pagerank of vertex m in the iteration n

![equation](https://latex.codecogs.com/gif.latex?\inline&space;v_{co}) is any vertex that
edge ![equation](https://latex.codecogs.com/gif.latex?\inline&space;(v_{co},&space;v_{m})) exists  
e.g. ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{2}(v_{2})&space;=&space;\frac{P_{1}(v_{1})}{1}&plus;\frac{P_{1}(v_{3})}{5})

All the states a worker X needs to know
are ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{co}))
and ![equation](https://latex.codecogs.com/gif.latex?\inline&space;outdegree(v_{co}))
. ![equation](https://latex.codecogs.com/gif.latex?\inline&space;outdegree(v_{co})) are constant,
but ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{co})) will be updated in each iteration.
Worse, worker X is probably not responsible for ![equation](https://latex.codecogs.com/gif.latex?\inline&space;v_{co}).
So other workers have to send calculated ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{co}))
back to worker X. E.g. worker A sends ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{7})) to
worker B, worker C sends ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{1})) to worker B in
iteration n.  
As long as worker B has collected ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{1}))
and ![equation](https://latex.codecogs.com/gif.latex?\inline&space;P_{n}(v_{7})), worker B can start iteration n+1.

### Depolyment

![equation](https://drive.google.com/uc?export=view&id=1Yj-XJr5XaaVHGVF1giFsl-AyU9I9sarQ)
preprocess.py, invoker.py, server.py can be deployed on different ec2 instances. client.py and softwareforwarder.py
should be deployed to the same AWS Lambda function.

1. Download data from [Stanford Large Network Dataset Collection](https://snap.stanford.edu/data/soc-Pokec.html)
2. Put preprocess.py, invoker.py, server.py in an ec2 instance with enough performance, check configuration. Create a S3
   bucket.
3. Deploy client.py and softwareforwarder.py to AWS Lambda

### Execution

1. Run preprocess.py to generate dataset from raw data download from Stanford Large Network Dataset Collection, if
   preprocess.py can not parse the raw data, adjust it. Dataset generated will be uploaded to S3.
2. Run server.py
3. Run invoker.py
