import math
import re
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def extract_words(s):
    # check nan
    if s is None: return ['']
    s = re.sub(r'[^\x00-\x7f]',r'', s)
    return tuple(s.strip('"').split(" "))


def count_word_by_total(data, field_idx):
    return data\
        .map(lambda x: extract_words(x[field_idx]))\
        .flatMap(lambda x: x)\
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[1], [x[0]]))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[0], False)


if __name__ == '__main__':
    # init spark
    conf = SparkConf().setAppName('Test')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # set log only error
    sc.setLogLevel("ERROR")  

    # read data
    news_file = 'hw2/data/News_Final.csv'
    news_data = sc.textFile(news_file)
    news_data = spark.read.csv(news_data).rdd.map(tuple)

    # remove header
    header = news_data.first()
    news_data = news_data.filter(lambda x: x != header)

    # title total word count
    title_wc = count_word_by_total(news_data, field_idx=1)

    # head line total word count
    head_line_wc = count_word_by_total(news_data, field_idx=2)

    print(title_wc.take(5))
    print(head_line_wc.take(5))