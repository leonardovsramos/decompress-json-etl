from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import gzip
import pandas as pd

spark = SparkSession.builder.getOrCreate()


# Python UDF
@F.udf(returnType=T.ByteType())
def decompress_python_udf(data: bytes) -> bytes:
    return gzip.decompress(data)


# Pandas UDF
@F.pandas_udf(T.ByteType())
def decompress_pandas_udf(data: pd.Series) -> pd.Series:
    return data.apply(lambda x: gzip.decompress(x))




