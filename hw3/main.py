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


def build_shingles(x, k):
    '''return shingle set from doc'''
    s = x[1]
    ls = list(set([tuple(s[i:(i+k)]) for i in range(len(s) - k + 1)]))
    return list(map(lambda e: (e, x[0]), ls))


def build_shingle_vector(x, num_doc):
    '''
    return row shingle vector (shingle, [0,0,1, ...])
    if shingle exist in doc set 1 else 0
    '''
    vector = [0 for _ in range(num_doc)]
    shingle, doc_idxs = x

    for i in doc_idxs:
        vector[i] = 1

    return (shingle, vector)


class Task1:
    def __init__(self):
        self.shingles = None
    
    @timer
    def run(self, docs, k):
        print('task1 running ...')

        num_doc = docs.count()

        self.shingles = docs\
            .zipWithIndex()\
            .map(lambda x: (x[1], extract_words(x[0])))\
            .flatMap(lambda x: build_shingles(x, k))\
            .groupByKey()\
            .map(lambda x: build_shingle_vector(x, num_doc))

        # self._save(num_doc)

    def _save(self, num_doc):
        print('task1 saving ...')

        # to output df
        hd = ['shingle'] + [f'doc_{i+1}' for i in range(num_doc)]
        df = self.shingles\
            .map(lambda x: ([str(x[0])] + x[1]))\
            .toDF(hd)

        df.show()

        # save file
        out_df = df.coalesce(1)
        out_df.write.csv(
            'hw3/output/task1', 
            mode='overwrite',
            header=True
        )


def create_hash_funcs(n):
    p = 224737
    a_cs = [i for i in range(10, 10+n)]
    b_cs = [i for i in range(2, 2+n)]
    hs = []
    for a, b in zip(a_cs, b_cs):
        def create_hash_func(a, b):
            def h(x):
                return (a*x + b) % p
            return h
        hs.append(create_hash_func(a, b))
    return hs


def build_hash_doc_list(row_idx, docs, hs):
    '''return ((hash_f_id, doc_id), hash_val)'''
    doc_ids = list(filter(
        lambda x: x is not None,
        map(lambda x : x[0] if x[1] == 1 else None, enumerate(docs))
    ))

    ls = list(map(
        lambda x: list(
            map(
                lambda d_id: ((x[0], d_id), x[1](row_idx)), 
                doc_ids
            )
        ),
        enumerate(hs)
    ))

    return ls


class Task2:
    def __init__(self):
        self.sig_mat = None

    @timer
    def run(self, shingles, num_doc, num_h):
        print('task2 running ...')
        hs = create_hash_funcs(num_h)
        
        self.sig_mat = shingles\
            .map(lambda x: x[1])\
            .zipWithIndex()\
            .flatMap(lambda x: build_hash_doc_list(x[1], x[0], hs))\
            .map(lambda x: x[0])\
            .reduceByKey(lambda a, b: a if a < b else b)\
            .sortBy(lambda x: (x[0][0], x[0][1]))\
            .map(lambda x: (x[0][0], x[1]))\
            .groupByKey()\
            .map(lambda x: list(x[1]))

        self._save(num_doc)
        

    def _save(self, num_doc):
        hd = [f'doc_{i}' for i in range(num_doc)]
        df = self.sig_mat.toDF(hd)
        df.select(df.columns[:3]).show(3)

        # out_df = df.coalesce(1)
        # out_df.write.csv(
        #     'hw3/output/task2', 
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
    num_doc = len(docs)
    docs = sc.parallelize(docs)

    # task1
    task1 = Task1()
    task1.run(docs, k=10)

    # task2
    task2 = Task2()
    task2.run(task1.shingles, num_doc, num_h=3)