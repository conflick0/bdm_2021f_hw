import re
import string
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def read_csv(spark, file_path):
    # read csv file
    data = spark.read\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .csv(file_path).rdd.map(tuple)
    
    # remove header
    header = data.first()
    data = data.filter(lambda x: x != header)

    return data


def extract_words(s):
    if s is None: return ['']
    s = re.sub(r'[^\x00-\x7f]',r'', s)
    s = s.translate(str.maketrans('', '', string.punctuation))
    return s.split()


def extract_words_with_key(k, v):
    return list(map(
        lambda x: ((k, x), 1), extract_words(v)
    ))


def count_by_total(data, field_idx):
    return data\
        .map(lambda x: extract_words(x[field_idx]))\
        .flatMap(lambda x: x)\
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[1], False)


def count_by_topic(data, field_idx):
    return data\
        .map(lambda x: extract_words_with_key(
                x[4],
                x[field_idx]
            )
        )\
        .flatMap(lambda x: x)\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0][0], [(x[0][1], x[1])]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (
                x[0], 
                sorted(x[1], key=lambda e: e[1], reverse=True)
            )
        )\
        .map(lambda x: (x[0], x[1][:10]))


def count_by_date(data, field_idx):
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
        .map(lambda x: (
                x[0], 
                sorted(x[1], key=lambda e: e[1], reverse=True)
            )
        )\
        .sortBy(lambda x: int(x[0].replace('-', '')))\
        .map(lambda x: (x[0], x[1][:10]))\


class WordCount:
    def __init__(self):
        self.by_total = None
        self.by_topic = None
        self.by_date = None


class Task1:
    def __init__(self, news_data):
        self.title_wc = WordCount()
        self.head_line_wc = WordCount()
        self.news_data = news_data
        
    def run(self):
        title_idx = 1
        head_line_idx = 2

        self.title_wc.by_total = count_by_total(
            self.news_data, title_idx
        )

        self.title_wc.by_topic = count_by_topic(
            self.news_data, title_idx
        )

        self.title_wc.by_date = count_by_date(
            self.news_data, title_idx
        )

        self.head_line_wc.by_total = count_by_total(
            self.news_data, head_line_idx
        )

        self.head_line_wc.by_topic = count_by_topic(
            self.news_data, head_line_idx
        )

        self.head_line_wc.by_date = count_by_date(
            self.news_data, head_line_idx
        )


if __name__ == '__main__':
    # init spark
    conf = SparkConf().setAppName('Test')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # set log only error
    sc.setLogLevel("ERROR")

    # read data
    news_file = 'hw2/data/News_Final.csv'
    news_data = read_csv(spark, news_file)
    
    task1 = Task1(news_data)
    task1.run()

    print(task1.title_wc.by_total.take(5))
    print(task1.title_wc.by_topic.take(4))
    print(task1.title_wc.by_date.take(4))
    print(task1.head_line_wc.by_total.take(5))
    print(task1.head_line_wc.by_topic.take(4))
    print(task1.head_line_wc.by_date.take(4))