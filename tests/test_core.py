import unittest

from mock import patch

from pyspark.sql import DataFrame, SparkSession

from bumblebee.core import Driver
from bumblebee import SchemaMappers as SM

schema_path = 'tests/schema/'
data_path = 'tests/data/'

spark = SparkSession.builder.getOrCreate()


class TestCore(unittest.TestCase):

    @patch('bumblebee.reader.SparkSession.sql')
    def test_driver_read_hive_is_dataframe_wo_conditions(self, mock_spark_sql):
        d = [{'foo':'bar'}]
        mock_spark_sql.return_value = spark.createDataFrame(d)

        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, SM.big_query)
        driver.read()
        self.assertTrue(isinstance(driver.df, DataFrame))

    def test_driver_read_table_schema(self):
        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, SM.big_query)
        self.assertEqual(driver.schema, {"col_string": "string",
                                         "col_integer": "integer",
                                         "col_float": "double",
                                         "col_date": "date",
                                         "col_datetime": "timestamp",
                                         "col_boolean": "boolean"})

    def test_driver_db_name(self):
        path = schema_path + 'default.test.json'
        driver = Driver('hive', path, SM.big_query)
        self.assertEqual(driver.db_name, 'default')

    def test_driver_table_name(self):
        path = schema_path + 'default.test.json'
        driver = Driver('hive', path, SM.big_query)
        self.assertEqual(driver.table_name, 'test')


