import datetime
import unittest

from mock import patch

from pyspark.sql import DataFrame, SparkSession, Row

from bumblebee.core import Driver

schema_path = 'tests/schema/'
data_path = 'tests/data/'

spark = SparkSession.builder.getOrCreate()


class TestCoreBasic(unittest.TestCase):

    schema_mapper='bq'

    def test_driver_df_not_ready_error(self):
        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, self.schema_mapper)
        with self.assertRaises(ValueError):
            driver.df

    @patch('bumblebee.reader.SparkSession.sql')
    def test_driver_read_hive_is_dataframe_wo_conditions(self, mock_spark_sql):
        d = [{'foo':'bar'}]
        mock_spark_sql.return_value = spark.createDataFrame(d)

        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, self.schema_mapper)
        driver.read()
        self.assertTrue(isinstance(driver.df, DataFrame))

    @patch('bumblebee.reader.SparkSession.sql')
    def test_driver_read_hive_is_dataframe_w_conditions(self, mock_spark_sql):
        data = [{"col_string": "string",
                 "col_integer": 5566,
                 "col_float": 5566.5566,
                 "col_date": "1995-01-01",
                 "col_datetime": "1995-01-01 00:01:01",
                 "col_boolean": True}]
        mock_spark_sql.return_value = spark.createDataFrame(data)

        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, self.schema_mapper)
        condition = "col_date = '1995-01-01'"
        driver.read(condition=condition)

        self.assertTrue(isinstance(driver.df, DataFrame))
        mock_spark_sql.assert_called_with("select * from `default`.`simple` where col_date = '1995-01-01'")
        mock_spark_sql.assert_called_once()

    def test_driver_read_table_schema(self):
        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, self.schema_mapper)
        self.assertEqual(driver.schema, {"col_string": "string",
                                         "col_integer": "integer",
                                         "col_float": "double",
                                         "col_date": "date",
                                         "col_datetime": "timestamp",
                                         "col_boolean": "boolean"})

    def test_driver_db_name(self):
        path = schema_path + 'default.test.json'
        driver = Driver('hive', path, self.schema_mapper)
        self.assertEqual(driver.db_name, 'default')

    def test_driver_table_name(self):
        path = schema_path + 'default.test.json'
        driver = Driver('hive', path, self.schema_mapper)
        self.assertEqual(driver.table_name, 'test')


class TestCoreValidator(unittest.TestCase):

    schema_mapper = 'bq'

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
        driver = Driver('hive', path, self.schema_mapper)
        valid_df = driver.read().validate().valid_df
        validate_data = valid_df.collect()

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
        driver = Driver('hive', path, self.schema_mapper)
        valid_df = driver.read().validate().valid_df
        validate_data = valid_df.collect()

        self.assertEqual(validate_data,
                         [Row(col_boolean=True, col_date=datetime.date(1995, 1, 1),
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=None, col_string='string')])

    @patch('bumblebee.reader.SparkSession.sql')
    def test_simple_data_validate_w_conditions(self, mock_spark_sql):
        data = [{"col_string": "string",
                 "col_integer": 5566,
                 "col_float": 5566.5566,
                 "col_date": "1995-01-01",
                 "col_datetime": "1995-01-01 00:01:01",
                 "col_boolean": True}]
        mock_spark_sql.return_value = spark.createDataFrame(data)

        simple_schema_path = 'tests/schema/' + 'simple.json'
        driver = Driver('hive', simple_schema_path, self.schema_mapper)
        condition = "col_date = '1995-01-01'"
        valid_df = driver.read(condition=condition).validate().valid_df

        self.assertTrue(isinstance(driver.df, DataFrame))
        mock_spark_sql.assert_called_with("select * from `default`.`simple` where col_date = '1995-01-01'")
        mock_spark_sql.assert_called_once()
        self.assertEqual(valid_df.collect(),
                         [Row(col_boolean=True, col_date=datetime.date(1995, 1, 1),
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=5566, col_string='string')])

    @patch('bumblebee.reader.SparkSession.sql')
    def test_simple_data_validate_column_name_number_in_string(self, mock_spark_sql):
        data = [{"1col_1string": "string",
                 "2col_2integer": "99-01-01",
                 "3col_3float": 5566.5566,
                 "4col_4date": "NULL",
                 "5col_5datetime": "1995-01-01 00:01:01",
                 "6col_6boolean": True}]
        mock_spark_sql.return_value = spark.createDataFrame(data)

        simple_schema_path = 'tests/schema/' + 'numeric_field.json'
        driver = Driver('hive', simple_schema_path, self.schema_mapper)
        valid_df = driver.read().validate().valid_df
        self.assertEqual(valid_df.collect(),
                         [Row(_1col_1string='string',
                              _2col_2integer=None,
                              _3col_3float=5566.5566,
                              _4col_4date=None,
                              _5col_5datetime=datetime.datetime(1995, 1, 1, 0, 1, 1),
                              _6col_6boolean=True)])
        self.assertEqual(valid_df.columns,
                         ['_1col_1string',
                          '_2col_2integer',
                          '_3col_3float',
                          '_4col_4date',
                          '_5col_5datetime',
                          '_6col_6boolean'])

    @patch('bumblebee.reader.SparkSession.sql')
    def test_data_validate_do_not_replace_field_start_with_number(self, mock_spark_sql):
        data = [{"1col_1string": "string",
                 "2col_2integer": "99-01-01",
                 "3col_3float": 5566.5566,
                 "4col_4date": "NULL",
                 "5col_5datetime": "1995-01-01 00:01:01",
                 "6col_6boolean": True}]
        mock_spark_sql.return_value = spark.createDataFrame(data)

        simple_schema_path = 'tests/schema/' + 'numeric_field.json'
        driver = Driver('hive', simple_schema_path, self.schema_mapper)
        valid_df = driver.read().validate(validate_schema=False).valid_df
        self.assertEqual(valid_df.columns,
                         ['1col_1string',
                          '2col_2integer',
                          '3col_3float',
                          '4col_4date',
                          '5col_5datetime',
                          '6col_6boolean'])
