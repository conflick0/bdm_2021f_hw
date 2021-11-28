import re
import string
import time
from functools import wraps
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark import SparkContext, SparkConf


def timer(func):
    '''cal execution time of func'''
    @wraps(func)
    def wrapper(*args, **kwargs):
        exec_time = time.time()
        func(*args, **kwargs)
        exec_time = time.time() - exec_time
        print(f'execution time: {exec_time} sec')
    return wrapper


def read_file(file_path):
    '''return file content text'''
    return spark.read.text(file_path, wholetext=True).rdd.collect()[0][0]


def read_docs(file_paths):
    '''return all docs'''
    docs = []
    for fp in file_paths:
        s = read_file(fp)
        s =  s.replace('\n','').replace('\r', '')
        bodys = re.findall(r'<BODY>(.+?)</BODY>', s)
        docs.extend(bodys)
    return docs


def extract_words(s):
    '''return word list from doc'''
    if s is None: return ['']
    s = re.sub(r'[^\x00-\x7f]',r'', s)
    s = s.translate(str.maketrans('', '', string.punctuation))
    return s.lower().split()


def build_shingles(s, k):
    '''return shingle set from doc'''
    return list(set([tuple(s[i:(i+k)]) for i in range(len(s) - k + 1)]))


if __name__ == '__main__':
    # init spark
    conf = SparkConf()\
        .setAppName('hw3')\
        .setMaster('spark://spark-1:7077')
    sc = SparkContext(conf=conf)
    
    spark = SparkSession(sc)

    # set log only error
    sc.setLogLevel("ERROR")

    file_paths = [f'hw3/data/reut2-{i:03}.sgm' for i in range(0, 22)]

    docs = read_docs(file_paths)
    
    docs = sc.parallelize(docs)

    doc_shingles = docs\
        .map(lambda x: extract_words(x))\
        .map(lambda x: build_shingles(x, k=8))

    print(doc_shingles.take(1))
