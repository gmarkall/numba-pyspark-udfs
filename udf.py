import pandas as pd

from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.getOrCreate()


@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    return series + 1


rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])
df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])

df.select(pandas_plus_one(df.a)).show()
