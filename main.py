from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class ColumnNames:
    global_active_power = 'Global_active_power'
    global_reactive_power = 'Global_reactive_power'
    voltage = 'Voltage'
    global_intensity = 'Global_intensity'


if __name__ == '__main__':
    data_file = 'household_power_consumption.txt'
    
    conf = SparkConf().setAppName('Test').setMaster('local')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    spark.sparkContext.setLogLevel('WARN') # turn off log info
    sc.setLogLevel("WARN")  # turn off log info

    df = spark.read.load(
        data_file,
        format='csv',
        sep=';',
        inferSchema='true',
        header='true',
        nullValue='?'
    )

    rows = df.select(
        ColumnNames.global_active_power,
        ColumnNames.global_reactive_power,
        ColumnNames.voltage,
        ColumnNames.global_intensity
    ).describe().show()
