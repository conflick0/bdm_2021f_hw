from pyspark import SparkContext, SparkConf

class Schema:
    global_active_power = 'Global_active_power'
    global_reactive_power = 'Global_reactive_power'
    voltage = 'Voltage'
    global_intensity = 'Global_intensity'


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
        (Schema.global_active_power, try_float(fields[2])),
        (Schema.global_reactive_power, try_float(fields[3])),
        (Schema.voltage, try_float(fields[4])),
        (Schema.global_intensity, try_float(fields[5]))
    )


if __name__ == '__main__':
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

    # # cal max, min and count
    max_val = data.reduceByKey(lambda a, b: a if a > b else b)
    min_val = data.reduceByKey(lambda a, b: a if a < b else b)
    count_val = data.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

    # # cal mean by map reduce
    # sum_val, count_val = data.map(lambda x: (x, 1))\
    #     .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    # mean_val = sum_val / count_val

    # # cal mean by agg
    # sum_val2, count_val2 = data.aggregate(
    #     (0, 0),
    #     (lambda acc, value: (acc[0] + value, acc[1] + 1)),
    #     (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
    # )
    # mean_val2 = sum_val2 / count_val2

    # # show result
    print(max_val.collect())
    print(min_val.collect())
    print(count_val.collect())
    # print(mean_val)
    # print(mean_val2)
