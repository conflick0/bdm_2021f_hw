import re
import string
import time
from functools import wraps
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        exec_time = time.time()
        func(*args, **kwargs)
        exec_time = time.time() - exec_time
        print(f'execution time: {exec_time} sec')
    return wrapper


def read_csv(file_path):
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
    
    @timer
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

    def show(self):
        print(self.title_wc.by_total.take(5))
        print(self.title_wc.by_topic.take(4))
        print(self.title_wc.by_date.take(4))
        print(self.head_line_wc.by_total.take(5))
        print(self.head_line_wc.by_topic.take(4))
        print(self.head_line_wc.by_date.take(4))


def cal_avg_pop(ts, n):
    ts = list(map(lambda x: float(x), ts))

    gp_n = 144 // n

    avgs = []
    for i in range(0, len(ts), gp_n):
        avgs.append(sum(ts[i:i+gp_n]) / gp_n)
    
    return sum(avgs) / n


def cal_avg_pop2(ts, n):
    return sum(list(map(lambda x: float(x), ts))) / n


def cal_avg_pop_by_hour(ts):
    # return cal_avg_pop(ts, n=48)
    return cal_avg_pop2(ts, n=48)


def cal_avg_pop_by_day(ts):
    # return cal_avg_pop(ts, n=2)
    return cal_avg_pop2(ts, n=2)


class Popularity:
    def __init__(self):
        self.by_hour = None
        self.by_day = None


class Task2:
    def __init__(self, fb_data, gp_data, li_data):
        self.fb_avg_pop = Popularity()
        self.gp_avg_pop = Popularity()
        self.li_avg_pop = Popularity()
        self.fb_data = fb_data
        self.gp_data = gp_data
        self.li_data = li_data
        
    @timer
    def run(self):
        self.fb_avg_pop.by_hour = self.fb_data.map(
            lambda r: (r[0], cal_avg_pop_by_hour(r[1:]))
        )

        self.fb_avg_pop.by_day = self.fb_data.map(
            lambda r: (r[0], cal_avg_pop_by_day(r[1:]))
        )

        self.gp_avg_pop.by_hour = self.gp_data.map(
            lambda r: (r[0], cal_avg_pop_by_hour(r[1:]))
        )

        self.gp_avg_pop.by_day = self.gp_data.map(
            lambda r: (r[0], cal_avg_pop_by_day(r[1:]))
        )

        self.li_avg_pop.by_hour = self.li_data.map(
            lambda r: (r[0], cal_avg_pop_by_hour(r[1:]))
        )

        self.li_avg_pop.by_day = self.li_data.map(
            lambda r: (r[0], cal_avg_pop_by_day(r[1:]))
        )

    def show(self):
        print(f'fb pop by hour: {self.fb_avg_pop.by_hour.take(4)}')
        print(f'fb pop by day: {self.fb_avg_pop.by_day.take(4)}')
        print(f'gp pop by hour: {self.gp_avg_pop.by_hour.take(4)}')
        print(f'gp pop by day: {self.gp_avg_pop.by_day.take(4)}')
        print(f'li pop by hour: {self.li_avg_pop.by_hour.take(4)}')
        print(f'li pop by day: {self.li_avg_pop.by_day.take(4)}')


def cal_sum_avg_sentiment(data):
    return data\
        .map(
            lambda r: (
                (('title', r[4]), (float(r[6]), 1)),
                (('head line', r[4]), (float(r[7]), 1)),
            )
        )\
        .flatMap(lambda x: x)\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .map(
            lambda x: (x[0][0], x[0][1] ,x[1][0], x[1][0]/x[1][1])
        )\
        .sortBy(lambda x: (-ord(x[0][0]), ord(x[1][0])))


class Task3:
    def __init__(self, news_data):
        self.sentiment = None
        self.news_data = news_data
    
    @timer
    def run(self):
        self.sentiment = cal_sum_avg_sentiment(
            self.news_data
        )

    def show(self):
        col_names = ['type', 'topic', 'sum', 'avg']
        s_df = self.sentiment.toDF(col_names)
        s_df.show(truncate=False)


if __name__ == '__main__':
    # init spark
    conf = SparkConf().setAppName('Test')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # set log only error
    sc.setLogLevel("ERROR")

    # read data
    news_data = read_csv('hw2/data/News_Final.csv')

    fb_e = read_csv('hw2/data/Facebook_Economy.csv')
    fb_m = read_csv('hw2/data/Facebook_Microsoft.csv')
    fb_o = read_csv('hw2/data/Facebook_Obama.csv')
    fb_p = read_csv('hw2/data/Facebook_Palestine.csv')

    gp_e = read_csv('hw2/data/GooglePlus_Economy.csv')
    gp_m = read_csv('hw2/data/GooglePlus_Microsoft.csv')
    gp_o = read_csv('hw2/data/GooglePlus_Obama.csv')
    gp_p = read_csv('hw2/data/GooglePlus_Palestine.csv')

    li_e = read_csv('hw2/data/LinkedIn_Economy.csv')
    li_m = read_csv('hw2/data/LinkedIn_Microsoft.csv')
    li_o = read_csv('hw2/data/LinkedIn_Obama.csv')
    li_p = read_csv('hw2/data/LinkedIn_Palestine.csv')

    fb_data = fb_e.union(fb_m).union(fb_o).union(fb_p)
    gp_data = gp_e.union(gp_m).union(gp_o).union(gp_p)
    li_data = li_e.union(li_m).union(li_o).union(li_p)

    # run task1
    task1 = Task1(news_data)
    task1.run()
    task1.show()

    # run task2
    task2 = Task2(fb_data, gp_data, li_data)
    task2.run()
    task2.show()

    # run task3
    task3 = Task3(news_data)
    task3.run()
    task3.show()