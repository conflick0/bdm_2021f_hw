import math
from pyspark import SparkContext, SparkConf


def try_float(value):
  try:
    value = float(value)
    return value
  except ValueError:
    return None


def parse_line(line):
    '''parsing the input data'''
    fields = line.split(';')
    return (
        (0, try_float(fields[2])),
        (1, try_float(fields[3])),
        (2, try_float(fields[4])),
        (3, try_float(fields[5]))
    )


if __name__ == '__main__':
    # schema
    schema = [
        'Global_active_power',
        'Global_reactive_power',
        'Voltage',
        'Global_intensity'
    ]

    # set data file path
    data_file = 'household_power_consumption.txt'

    # init spark
    conf = SparkConf().setAppName('Test').setMaster('local')
    sc = SparkContext(conf=conf)

    # turn off log info
    sc.setLogLevel("WARN")  

    # read data
    data = sc.textFile(data_file)

    # remove header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # extract target fields
    data = data.map(parse_line)

    # build key value pair (field_name, value)
    data = data.flatMap(lambda x: x)

    # remove null value
    data = data.filter(lambda x: x[1] is not None)

    # cal max
    max_val = data.reduceByKey(lambda a, b: a if a > b else b)\
        .map(lambda x: x[1]).collect()

    # cal min
    min_val = data.reduceByKey(lambda a, b: a if a < b else b)\
        .map(lambda x: x[1]).collect()
    
    # cal count
    count_val = data.map(lambda x: (x[0], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: x[1]).collect()

    # cal mean
    mean_val = data.mapValues(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0]/x[1])\
        .map(lambda x: x[1]).collect()
    
    # cal std
    std_val = data.map(lambda x: (x[0], (x[1] - mean_val[x[0]])**2))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: math.sqrt(x[1]/count_val[x[0]])).collect()

    # min max norm 
    data_norm = data.map(lambda x: (
                x[0], 
                (x[1] - min_val[x[0]])/(max_val[x[0]] - min_val[x[0]])
            )
        )

    # show result
    print(f'max: {max_val}')
    print(f'min: {min_val}')
    print(f'count: {count_val}')
    print(f'mean: {mean_val}')
    print(f'std: {std_val}')
    print(f'min max norm:\n{data_norm.take(8)}')
