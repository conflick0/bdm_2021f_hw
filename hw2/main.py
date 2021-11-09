import math
import re
import time
import string
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def extract_words(s):
    if s is None: return ['']
    s = re.sub(r'[^\x00-\x7f]',r'', s)
    s = s.translate(str.maketrans('', '', string.punctuation))
    return s.split()


def count_word_by_total(data, field_idx):
    return data\
        .map(lambda x: extract_words(x[field_idx]))\
        .flatMap(lambda x: x)\
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1], False)


def count_word_by_topic_flt(data, topic, field_idx):
    return data\
        .filter(lambda x: x[4] == topic)\
        .map(lambda x: extract_words(x[field_idx]))\
        .flatMap(lambda x: x)\
        .map(lambda w: (w, 1))\
        .reduceByKey(lambda a, b: a+b)\
        .sortBy(lambda x: x[1], False)


def extract_words_with_key(k, v):
    return list(map(
        lambda x: ((k, x), 1), extract_words(v)
    ))


def count_word_by_topic(data, field_idx):
    return data\
        .map(lambda x: extract_words_with_key(
                x[4],
                x[field_idx]
            )
        )\
        .flatMap(lambda x: x)\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1], False)\
        .map(lambda x: (x[0][0], [(x[0][1], x[1])]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0], x[1][:10]))\
        .sortBy(lambda x: x[0], False)


def count_word_by_date(data, field_idx):
    return data\
        .map(lambda x: extract_words_with_key(
                x[5].split()[0],
                x[field_idx]
            )
        )\
        .flatMap(lambda x: x)\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1], False)\
        .map(lambda x: (x[0][0], [(x[0][1], x[1])]))\
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
    news_data = spark.read\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .csv(news_data).rdd.map(tuple)

    # remove header
    header = news_data.first()
    news_data = news_data.filter(lambda x: x != header)

    # total word count
    # title_wc = count_word_by_total(news_data, field_idx=1)

    title_topic_wc = count_word_by_topic(news_data,1)

    # word count by topic
    title_topic_wc_flt = count_word_by_topic_flt(
        news_data, 
        'obama', 
        field_idx=1
    )
    
    # print(title_wc.take(5))
    print(title_topic_wc.take(4))
    print(title_topic_wc_flt.take(10))