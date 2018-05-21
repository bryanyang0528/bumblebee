import datetime
import unittest

from mock import patch

from pyspark.sql import DataFrame, SparkSession, Row

from bumblebee.core import Driver
from bumblebee import SchemaMappers as SM

schema_path = 'tests/schema/'
data_path = 'tests/data/'

spark = SparkSession.builder.getOrCreate()


class TestCoreBasic(unittest.TestCase):

    def test_driver_df_not_ready_error(self):
        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, SM.big_query)
        with self.assertRaises(ValueError):
            driver.df

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


class TestCoreValidator(unittest.TestCase):

    @patch('bumblebee.reader.SparkSession.sql')
    def test_simple_data_validate_pass_all(self, mock_spark_sql):
        data = [{"col_string": "string",
                 "col_integer": 5566,
                 "col_float": 5566.5566,
                 "col_date": "1995-01-01",
                 "col_datetime": "1995-01-01 00:01:01",
                 "col_boolean": True}]
        mock_spark_sql.return_value = spark.createDataFrame(data)

        path = schema_path + 'default.test.json'
        driver = Driver('hive', path, SM.big_query)
        validate_df = driver.read().validate().df
        validate_data = validate_df.collect()

        self.assertEqual(validate_data,
                         [Row(col_boolean=True, col_date=datetime.date(1995, 1, 1),
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=5566, col_string='string')])

    @patch('bumblebee.reader.SparkSession.sql')
    def test_simple_data_validate_invalidate_int_null(self, mock_spark_sql):
        data = [{"col_string": "string",
                 "col_integer": "NULL",
                 "col_float": 5566.5566,
                 "col_date": "1995-01-01",
                 "col_datetime": "1995-01-01 00:01:01",
                 "col_boolean": True}]
        mock_spark_sql.return_value = spark.createDataFrame(data)

        path = schema_path + 'default.test.json'
        driver = Driver('hive', path, SM.big_query)
        validate_df = driver.read().validate().df
        validate_data = validate_df.collect()

        self.assertEqual(validate_data,
                         [Row(col_boolean=True, col_date=datetime.date(1995, 1, 1),
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=None, col_string='string')])
