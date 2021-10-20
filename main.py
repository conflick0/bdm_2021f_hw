from pyspark.sql import SparkSession


class Task1:
    def __init__(self, col_name):
        self.col_name = col_name
        self.max = None
        self.min = None
        self.count = None


def cal_task1(df, col_name):
    task1 = Task1(col_name)
    task1.max = df.agg({'Global_active_power': 'max'}).first()[0]
    task1.min = df.agg({'Global_active_power': 'min'}).first()[0]
    task1.count = df.agg({'Global_active_power': 'count'}).first()[0]
    return task1


class ColumnNames:
    global_active_power = 'Global_active_power'
    global_reactive_power = 'Global_reactive_power'
    voltage = 'Voltage'
    global_intensity = 'Global_intensity'


if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataFrame").getOrCreate()
    data_file = 'household_power_consumption.txt'

    df = spark.read.load(
        data_file,
        format='csv',
        sep=';',
        inferSchema='true',
        header='true',
        nullValue='?'
    )

    gap_task1_result = cal_task1(df, ColumnNames.global_active_power)
    print(f'{gap_task1_result.col_name}: '
          f'max:{gap_task1_result.max}, '
          f'min:{gap_task1_result.min}, '
          f'count:{gap_task1_result.count}')
