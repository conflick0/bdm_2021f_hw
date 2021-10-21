from pyspark import SparkContext, SparkConf

class ColumnNames:
    global_active_power = 'Global_active_power'
    global_reactive_power = 'Global_reactive_power'
    voltage = 'Voltage'
    global_intensity = 'Global_intensity'


def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False


def parseLine(line):
    '''parsing the input data'''
    fields = line.split(';')
    datetime = ''.join([fields[0], fields[1]])

    target_idxs = [2, 3]
    target_fields = [datetime]
    for i in target_idxs:
        if isfloat(fields[i]):
            target_fields.append(float(fields[i]))
        else:
            target_fields.append(None)

    return target_fields[1]


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
    data = data.filter(lambda row: row != header)

    # extract target fields
    data = data.map(parseLine)

    # remove null value
    data = data.filter(lambda x: x is not None)

    # cal max, min and count
    max_val = data.reduce(lambda a, b: a if a > b else b)
    min_val = data.reduce(lambda a, b: a if a < b else b)
    count_val = data.map(lambda x: 1).reduce(lambda a, b: a + b)

    # show result
    print(max_val)
    print(min_val)
    print(count_val)
