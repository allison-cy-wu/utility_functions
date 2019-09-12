from pyspark.sql.functions import rand
from pyspark.sql.functions import split, explode, col, ltrim, rtrim, coalesce, countDistinct, broadcast
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from connect2Databricks.spark_init import spark_init
if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, _ = spark_init()

sc = spark.sparkContext


def permute_columns(df,
                    column_to_order: str,
                    ind_permute: bool = False,
                    columns_to_permute: list = list()):
    """
    Author: Allison Wu
    Description: This function permutes the columns specified in columns_to_permute
    :param df:
    :param column_to_order:
    :param ind_permute:
    :param columns_to_permute:
    :return: permuted_df
    """
    window = Window.partitionBy().orderBy(col(column_to_order))
    window_rand = Window.partitionBy().orderBy(rand())

    df = df. \
        withColumn('id', func.row_number().over(window)). \
        withColumn('rand_id', func.row_number().over(window_rand))

    rand_df = df. \
        select(['rand_id'] + columns_to_permute).\
        withColumnRenamed('rand_id', 'id')

    for c in columns_to_permute:
        rand_df = rand_df.\
            withColumnRenamed(c, f'rand_{c}')

    permuted_df = df.join(rand_df, ['id'], how = 'inner')

    return permuted_df
