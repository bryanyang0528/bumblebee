import unittest

from mock import patch

from pyspark.sql import DataFrame, SparkSession
from bumblebee import Table, HiveReader
from bumblebee import SchemaMappers as ST

schema_path = 'tests/schema/'
data_path = 'tests/data/'

spark = SparkSession.builder.getOrCreate()


class TestHiveReader(unittest.TestCase):

    def setUp(self):
        pass

    @patch('bumblebee.reader.SparkSession.sql')
    def test_reader_is_dataframe(self, mock_spark_sql):
        d = [{'foo':'bar'}]
        mock_spark_sql.return_value = spark.createDataFrame(d)

        reader = HiveReader()
        db = 'default'
        table = 'test'
        df = reader.select(db, table)
        mock_spark_sql.assert_called_with("select * from default.test")
        mock_spark_sql.assert_called_once()
        self.assertTrue(isinstance(df, DataFrame))

    @patch('bumblebee.reader.SparkSession.sql')
    def test_reader_wo_db(self, mock_spark_sql):
        d = [{'foo':'bar'}]
        mock_spark_sql.return_value = spark.createDataFrame(d)

        reader = HiveReader()
        table = 'test'
        df = reader.select(table=table)
        mock_spark_sql.assert_called_with("select * from default.test")
        mock_spark_sql.assert_called_once()
        self.assertTrue(isinstance(df, DataFrame))

    @patch('bumblebee.reader.SparkSession.sql')
    def test_reader_wo_table(self, mock_spark_sql):
        d = [{'foo':'bar'}]
        mock_spark_sql.return_value = spark.createDataFrame(d)

        reader = HiveReader()
        db = 'default'
        with self.assertRaises(ValueError):
            df = reader.select(db=db)
            mock_spark_sql.assert_not_called()

    @patch('bumblebee.reader.SparkSession.sql')
    def test_reader_is_dataframe_w_partition(self, mock_spark_sql):
        d = [{'foo': 'bar', 'dt': '2018-01-01'},
             {'foo': 'qoo', 'dt': '2018-01-02'}]
        mock_spark_sql.return_value = spark.createDataFrame(d)

        reader = HiveReader()
        db = 'default'
        table = 'test'
        condition = "dt > '2018-01-01'"
        df = reader.select(db, table, condition=condition)
        mock_spark_sql.assert_called_with("select * from default.test where dt > '2018-01-01'")
        mock_spark_sql.assert_called_once()
        self.assertTrue(isinstance(df, DataFrame))

    @patch('bumblebee.reader.SparkSession.sql')
    def test_read_df_w_invalid_partition_not_basestring(self, mock_spark_sql):
        d = [{'foo': 'bar'}]
        mock_spark_sql.return_value = spark.createDataFrame(d)

        reader = HiveReader()
        db = 'default'
        table = 'test'
        condition = ["dt > '2018-01-01'", "dt < '2018-01-02'"]
        with self.assertRaises(ValueError):
            df = reader.select(db, table, condition=condition)
            mock_spark_sql.assert_not_called()

    def test_reader_w_table(self):
        path = schema_path + 'default.test.json'
        table = Table(path, 'bq')
        reader = HiveReader(table)
        self.assertEqual(reader.table_name, table.name)
        self.assertEqual(reader.db_name, table.db_name)




