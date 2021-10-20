from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")  # set log error

data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

print(f'### Results:\n {rdd.collect()}')
sc.stop()
