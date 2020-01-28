from utility_functions.benchmark import timer
from pyspark.sql.dataframe import DataFrame as sparkDataFrame
from typing import List
from connect2Databricks.spark_init import spark_init
if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, _ = spark_init()

sc = spark.sparkContext


def add_col_prefix(
        prefix: str,
        df: sparkDataFrame,
) -> sparkDataFrame:
    for col in df.columns:
        df = df.withColumnRenamed(col, f'{prefix}{col}')
    return df


def unpack_df_col(
        df: sparkDataFrame,
        col_name: str,
) -> List:

    df = df.withColumnRenamed(col_name, 'col_to_extract')
    list_col_contents = [row.col_to_extract for row in  df.select('col_to_extract').collect()]

    return list_col_contents


def has_column(df, col_name: str):
    if col_name in df.columns:
        return True
    else:
        return False


@timer
def clone(df):
    """
    Author: Allison Wu
    Description: avoid the resolved attributes missing problem.
    see https://issues.apache.org/jira/browse/SPARK-14948.
    :param df:
    :return:
    """
    df = spark.createDataFrame(df.rdd, df.schema)
    return df


def clear_cache():
    sqlContext.clearCache()


def pandas_to_df(pd_df,cache=True):
    """
    Author: Rich Winkler
    Description: Creates a Spark DataFrame out of a given Pandas DataFrame
    :param pd_df: The Pandas DataFrame to be converted to Spark
    :param cache: Default True, whether to cache the resulting dataframe
    :return: df, the Spark DataFrame
    """
    if cache:
        df = spark.createDataFrame(pd_df).cache()
    else:
        df = spark.createDataFrame(pd_df)

    return df

@timer
def rdd_to_df(rdd, cache = True):
    if cache:
        df = sc.parallelize(rdd).cache().toDF()
    else:
        df = sc.parallelize(rdd).toDF()

    sqlContext.clearCache()
    return df


@timer
def collect_and_cache(df):
    if 'spark' not in locals():
        print('Environment: Databricks-Connect')
        _, _, setting = spark_init()

    if 'local' in setting:
        df = rdd_to_df(df.collect())
    else:
        df = df.cache()

    return df


