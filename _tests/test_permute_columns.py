import unittest
from unittest import TestCase
from utility_functions.stats_functions import permute_columns
from utility_functions.databricks_uf import has_column
from connect2Databricks.spark_init import spark_init
if 'spark' not in locals():
    spark, sqlContext, setting = spark_init()

sc = spark.sparkContext


class TestPermuteColumns(TestCase):
    def test_permute_columns(self):
        data = spark.createDataFrame([(1, 'a', 'a'),
                                      (2, 'b', 'b'),
                                      (3, 'c', 'c'),
                                      (4, 'd', 'd'),
                                      (5, 'e', 'e')],
                                     ['id', 'col1', 'col2'])
        permuted_data = permute_columns(data,
                                        columns_to_permute = ['col1', 'col2'],
                                        column_to_order = 'id',
                                        ind_permute = False)

        permuted_data.show()

        self.assertTrue(has_column(permuted_data, 'rand_id'))
        self.assertTrue(has_column(permuted_data, 'rand_col1'))
        self.assertTrue(has_column(permuted_data, 'rand_col2'))
        self.assertEqual(permuted_data.select('rand_col1').collect(), permuted_data.select('rand_col2').collect())
        self.assertNotEqual(permuted_data.select('col1').collect(), permuted_data.select('rand_col1').collect())


if __name__ == '__main__':
    unittest.main()
