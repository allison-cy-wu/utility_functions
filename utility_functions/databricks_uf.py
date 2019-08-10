from utility_functions.benchmark import timer
import csv
import io
from connect2Databricks.spark_init import spark_init
if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, setting = spark_init()

sc = spark.sparkContext


@timer
def rdd_to_df(rdd, cache = True):
    if cache:
        df = sc.parallelize(rdd).cache().toDF()
    else:
        df = sc.parallelize(rdd).toDF()

    sqlContext.clearCache()
    return df


def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip()


