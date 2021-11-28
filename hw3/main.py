import re
import string
import time
from functools import wraps
from itertools import zip_longest
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


class Task1:
    def __init__(self):
        self.shingles = None
    
    @timer
    def cal_shingles(self, docs, k):
        self.shingles = docs\
            .map(lambda x: extract_words(x))\
            .map(lambda x: build_shingles(x, k))

    def save(self):
        # to df for save file
        out_ls = list(
            zip_longest(*self.shingles.collect(), fillvalue='')
        )

        header = [f'doc_{i+1}' for i in range(len(out_ls[0]))]

        # df = sc.parallelize(out_ls).toDF()

        # df = sc.parallelize(out_ls).toDF(
        #     [f'doc_{i+1}' for i in range(len(out_ls[0]))]
        # )

        # df.show(1, truncate=False, vertical=True)

        # out_df =  df.coalesce(1)
        # out_df.write.csv(
        #     'hw3/output/task1', 
        #     mode='overwrite',
        #     header=True
        # )


if __name__ == '__main__':
    # init spark
    conf = SparkConf()\
        .setAppName('hw3')\
        .setMaster('spark://spark-1:7077')
    sc = SparkContext(conf=conf)
    
    spark = SparkSession(sc)

    # set log only error
    sc.setLogLevel("ERROR")

    # set file paths
    file_paths = [f'hw3/data/reut2-{i:03}.sgm' for i in range(0, 22)]

    # read docs
    docs = read_docs(file_paths)
    docs = sc.parallelize(docs)

    # task1
    task1 = Task1()
    task1.cal_shingles(docs, k=8)
    task1.save()