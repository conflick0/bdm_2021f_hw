import math
from pyspark.sql import SparkSession
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


def cal_min_max_norm(val, max_val, min_val):
    if val is None: return val
    return (val - min_val) / (max_val - min_val)


def cal_row_min_max_norm(xs, max_vals, min_vals):
    return tuple(map(
        lambda x: cal_min_max_norm(
            x[1],
            max_vals[x[0]], 
            min_vals[x[0]]),
        xs
    ))


def create_result_df(result_vals, category, schema):
    '''create df for max, min, count, mean and std result'''
    result_rows = []
    for c, vs in zip(category, result_vals):
        row = [c]
        row.extend([str(v) for v in vs])
        result_rows.append(row)
    
    schema_row = ['category']
    schema_row.extend(schema)
    
    return spark.createDataFrame(
        result_rows,
        schema_row
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
    spark = SparkSession(sc)

    # set log only error
    sc.setLogLevel("ERROR")  

    # read data
    data = sc.textFile(data_file)

    # remove header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # extract target fields
    raw_data = data.map(parse_line)

    # build key value pair (field_name, value)
    data = raw_data.flatMap(lambda x: x)

    # remove null value
    data = data.filter(lambda x: x[1] is not None)

    # cal max
    max_vals = data.reduceByKey(lambda a, b: a if a > b else b)\
        .map(lambda x: x[1]).collect()

    # cal min
    min_vals = data.reduceByKey(lambda a, b: a if a < b else b)\
        .map(lambda x: x[1]).collect()
    
    # cal count
    count_vals = data.map(lambda x: (x[0], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: x[1]).collect()

    # cal mean
    mean_vals = data.mapValues(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0]/x[1])\
        .map(lambda x: x[1]).collect()
    
    # cal std
    std_vals = data.map(lambda x: (x[0], (x[1] - mean_vals[x[0]])**2))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: math.sqrt(x[1]/count_vals[x[0]])).collect()

    # cal min max norm 
    norm_data = raw_data.map(
        lambda x: cal_row_min_max_norm(x, max_vals, min_vals)
    )

    # convert rdd to df
    norm_df = norm_data.toDF(schema)
    
    # create df for min, max, count, mean and count result
    result_df = create_result_df(
        [max_vals, min_vals, count_vals, mean_vals, std_vals],
        ['max', 'min', 'mean', 'count', 'mean', 'std'],
        schema
    )
    
    # show result
    print('result:')
    result_df.show()
    print(f'min max norm:')
    norm_df.show(5)
