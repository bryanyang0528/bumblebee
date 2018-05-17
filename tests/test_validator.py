import json
import datetime
import unittest

from pyspark.sql import SparkSession, Row

from bumblebee import Validator

schema_path = 'tests/schema/'
data_path = 'tests/data/'

spark = SparkSession.builder.getOrCreate()


class TestValidator(unittest.TestCase):

    simple_schema = {"col_string": "string",
                     "col_integer": "integer",
                     "col_float": "double",
                     "col_date": "date",
                     "col_datetime": "timestamp",
                     "col_boolean": "boolean"}

    def setUp(self):
        pass

    def test_simple_data_validate(self):
        data_for_simple_schema_path = data_path + 'simple.json'
        with open(data_for_simple_schema_path) as f:
            data = json.load(f)

        simple_df = spark.createDataFrame(data)

        validate_data = Validator.validate_data(simple_df, self.simple_schema).collect()
        self.assertEqual(validate_data,
                         [Row(col_boolean=True, col_date=datetime.date(1995, 1, 1),
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=5566, col_string='string')])

    def test_simple_data_validate_invalid(self):
        data_for_simple_schema_path = data_path + 'simple_invalid.json'

        with open(data_for_simple_schema_path) as f:
            data = json.load(f)
        simple_invalid_df = spark.createDataFrame(data)

        validate_data = Validator.validate_data(simple_invalid_df, self.simple_schema).collect()
        self.assertEqual(validate_data,
                         [Row(col_boolean=True, col_date=datetime.date(1995, 1, 1),
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=None, col_string='string')])

    def test_simple_data_validate_invalidate_date_null(self):
        data = [{"col_string": "string",
                 "col_integer": "NULL",
                 "col_float": 5566.5566,
                 "col_date": "NULL",
                 "col_datetime": "1995-01-01 00:01:01",
                 "col_boolean": True}]
        df = spark.createDataFrame(data)

        validate_data = Validator.validate_data(df, self.simple_schema).collect()
        self.assertEqual(validate_data,
                         [Row(col_boolean=True, col_date=None,
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=None, col_string='string')])

    def test_simple_data_validate_invalidate_date_format(self):
        data = [{"col_string": "string",
                 "col_integer": "99-01-01",
                 "col_float": 5566.5566,
                 "col_date": "NULL",
                 "col_datetime": "1995-01-01 00:01:01",
                 "col_boolean": True}]
        df = spark.createDataFrame(data)

        schema = {"col_string": "string",
                  "col_integer": "integer",
                  "col_float": "double",
                  "col_date": "date",
                  "col_datetime": "timestamp",
                  "col_boolean": "boolean"}

        validate_data = Validator.validate_data(df, schema).collect()
        self.assertEqual(validate_data,
                         [Row(col_boolean=True, col_date=None,
                              col_datetime=datetime.datetime(1995, 1, 1, 0, 1, 1), col_float=5566.5566,
                              col_integer=None, col_string='string')])
