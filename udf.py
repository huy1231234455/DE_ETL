from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

spark= SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]") \
    .config("spark.executor.memory","4g") \
    .getOrCreate()
data = [("11/12/2025",),("27/02.2014",),("2023.01.09",),("28-12-2005",)]
df = spark.createDataFrame(data , ["date"])
def split_date(data):
    date_str = data.replace(".", "/").replace("-", "/")  # Replace ve "/"
    parts = date_str.split("/")

    if len(parts[0]) == 4:  # yyyy.MM.dd
        year, month, day = parts
    elif len(parts[2]) == 4:  # dd/MM/yyyy or dd-MM-yyyy
        day, month, year = parts
    else:
        return None, None, None

    return day, month, year

get_day_udf = udf(lambda date: split_date(date)[0], StringType())
get_month_udf = udf(lambda date: split_date(date)[1], StringType())
get_year_udf = udf(lambda date:split_date(date)[2], StringType())

df = df.withColumn("day", get_day_udf(df.date)) \
       .withColumn("month", get_month_udf(df.date)) \
       .withColumn("year", get_year_udf(df.date))

df.show()