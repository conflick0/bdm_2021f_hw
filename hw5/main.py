import time
from functools import wraps

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark import SparkContext, SparkConf


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        exec_time = time.time()
        func(*args, **kwargs)
        exec_time = time.time() - exec_time
        print(f'execution time: {exec_time} sec')
    return wrapper


@timer
def task1(data):
    '''return a sorted list of pages with their out-degrees'''
    print('task1 running ...')

    out = data\
        .map(lambda x: (x[0], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1], False)
    
    print('task1 output ...')
    df = out.toDF(['NodeID', 'out-degree'])
    df.show(20)


@timer
def task2(data):
    '''return a sorted list of pages with their in-degrees'''
    print('task2 running ...')

    out = data\
        .map(lambda x: (x[1], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1], False)
    
    print('task2 output ...')
    df = out.toDF(['NodeID', 'in-degree'])
    df.show(20)


@timer
def task3(data, node_v):
    '''
    Given a node v, 
    output the list of nodes that v points to, 
    and the list of nodes that points to v. 
    '''

    print('task3 running ...')

    # v to other nodes
    to_node = data\
        .filter(lambda x: int(x[0]) == node_v)\
        .groupByKey()\
        .map(lambda x: sorted(list(x[1])))

    # other nodes to v
    from_node = data\
        .filter(lambda x: int(x[1]) == node_v)\
        .map(lambda x: (x[1], x[0]))\
        .groupByKey()\
        .map(lambda x: sorted(list(x[1])))
    
    print('task3 output ...')
    print(f'node v: {node_v}')
    print('nodes that v points to:')
    print(to_node.take(1)[0])
    print('nodes that point to v:')
    print(from_node.take(1)[0])


if __name__ == '__main__':
    # set spark config
    conf = SparkConf()\
        .setAppName('hw5')\
        .setMaster('spark://spark-1:7077')
    
    # set spark context
    sc = SparkContext(conf=conf)
    
    # set log only error
    sc.setLogLevel("ERROR")

    # init spark session
    spark = SparkSession(sc)

    # read file
    data = sc.textFile('hw5/data/web-Google.txt')

    # remove headers and parse data
    data = data\
        .zipWithIndex()\
        .filter(lambda x: x[1] > 3)\
        .map(lambda x: tuple(x[0].split('\t')))

    # task1
    task1(data)

    # task2
    task2(data)

    # taks3
    task3(data, node_v=0)