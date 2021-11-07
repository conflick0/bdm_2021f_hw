import math
import re
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def parse_line(line):
    '''parsing the input data'''
    fields = line.split(',')
    return (
        fields[0],
        fields[1],
        fields[2],
        fields[4],
        fields[5],
        fields[6],
        fields[7],
    )


def is_nan(x):
    return x != x


def extract_words(sentence):
    # check nan
    if is_nan(sentence): return ['']
    
    # remove hex string
    sentence = re.sub(r'[^\x00-\x7f]',r'', sentence) 
    
    return sentence.strip('"').split(" ")


if __name__ == '__main__':
    # init spark
    conf = SparkConf().setAppName('Test')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # set log only error
    sc.setLogLevel("ERROR")  

    # read data
    data_file_path = 'hw2/data/News_Final.csv'
    data = sc.textFile(data_file_path)

    # remove header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # extract target fields
    raw_data = data.map(parse_line)

    # title total word count
    title_wc = raw_data\
        .map(lambda x: extract_words(x[1]))\
        .flatMap(lambda x: x)\
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[1], [x[0]]))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[0], False)

    # head line total word count
    head_line_wc = raw_data\
        .map(lambda x: extract_words(x[2]))\
        .flatMap(lambda x: x)\
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[1], [x[0]]))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: x[0], False)
    

    print(title_wc.take(5))
    print(head_line_wc.take(5))