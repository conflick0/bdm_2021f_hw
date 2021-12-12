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


def read_file(file_path):
    data = spark.read\
        .option("delimiter", "::")\
        .csv(file_path)\
        .rdd.map(tuple)
    
    return data


@timer
def task1(movies, ratings):
    '''
    sorted in descending order of average rating score
    '''
    print('task1 running ...')

    movies = movies\
        .map(lambda x: (x[0], x[1]))

    ratings = ratings\
        .map(lambda x: (x[1], (float(x[2]), 1)))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .join(movies)\
        .sortBy(lambda x: (x[1][0], int(x[0])), False)\
        .map(lambda x: (x[1][1], x[1][0]))

    print('task1 output ...')

    df = ratings.toDF(['movie', 'score'])
    df.show(20, False)


@timer
def task2(movies, ratings, users):
    '''
    sorted in descending order of average rating score 
    grouped by gender, by age group, and by occupation
    '''
    print('task2 running ...')

    movies = movies\
        .map(lambda x: (x[0], x[1]))

    ratings = ratings\
        .map(lambda x: ((x[0], (x[1], float(x[2])))))

    users = users\
        .map(lambda x: (x[0], (x[1], x[2], x[3])))

    data = ratings.join(users)

    gd_gp = data\
        .map(lambda x: ((x[1][0][0], x[1][1][0]), (x[1][0][1], 1)))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .join(movies)\
        .sortBy(lambda x: (x[1][0][1], int(x[0])), False)\
        .map(lambda x: (x[1][1], x[1][0][0], x[1][0][1]))

    age_gp = data\
        .map(lambda x: ((x[1][0][0], x[1][1][1]), (x[1][0][1], 1)))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .join(movies)\
        .sortBy(lambda x: (x[1][0][1], int(x[0])), False)\
        .map(lambda x: (x[1][1], x[1][0][0], x[1][0][1]))


    ocp_gp = data\
        .map(lambda x: ((x[1][0][0], x[1][1][2]), (x[1][0][1], 1)))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .join(movies)\
        .sortBy(lambda x: (x[1][0][1], int(x[0])), False)\
        .map(lambda x: (x[1][1], x[1][0][0], x[1][0][1]))


    print('task2 output ...')

    gd_df = gd_gp.toDF(['movie', 'gender', 'score'])
    age_df = age_gp.toDF(['movie', 'age', 'score'])
    ocp_df = ocp_gp.toDF(['movie', 'occupation', 'score'])

    gd_df.show(20, False)
    age_df.show(20, False)
    ocp_df.show(20, False)


def extract_genres(x):
    uid, rat = x[1][0]
    genres = (x[1][1]).split('|')
    return list(map(lambda g: ((uid, g), (rat, 1)), genres))


@timer
def task3(movies, ratings):
    '''
    sorted in descending order of average rating score 
    of each user for all movies, and by genre
    '''
    print('task3 running ...')

    user_rats = ratings\
        .map(lambda x: (x[0], (float(x[2]), 1)))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .sortBy(lambda x: (x[1], int(x[0])), False)

    rats = ratings\
        .map(lambda x: (x[1], (x[0], float(x[2]))))
    
    movs = movies\
        .map(lambda x: (x[0], x[2]))

    user_genre_rats = rats\
        .join(movs)\
        .flatMap(lambda x: extract_genres(x))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .map(lambda x: (x[0][0], x[0][1], x[1]))\
        .sortBy(lambda x: (x[2], int(x[0])), False)

    print('task3 output ...')

    ur_df = user_rats.toDF(['user', 'score'])
    ugr_df = user_genre_rats.toDF(['user', 'genre', 'score'])
    
    ur_df.show(20, False)
    ugr_df.show(20, False)


if __name__ == '__main__':
    # set spark config
    conf = SparkConf()\
        .setAppName('hw4')\
        .setMaster('spark://spark-1:7077')
    
    # set spark context
    sc = SparkContext(conf=conf)
    
    # set log only error
    sc.setLogLevel("ERROR")

    # init spark session
    spark = SparkSession(sc)

    # read file
    ratings = read_file('hw4/data/ratings.dat')
    users = read_file('hw4/data/users.dat')
    movies = read_file('hw4/data/movies.dat')

    # task1
    task1(movies, ratings)

    # task2
    task2(movies, ratings, users)

    # taks3
    task3(movies, ratings)