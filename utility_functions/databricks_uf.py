from utility_functions.benchmark import timer
from connect2Databricks.spark_init import spark_init
if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, _ = spark_init()

sc = spark.sparkContext


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


