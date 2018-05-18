import unittest

from bumblebee import Table
from bumblebee import SchemaTypes as ST

schema_path = 'tests/schema/'


class TestSchemaParser(unittest.TestCase):

    def setUp(self):
        self.simple_schema_path = schema_path + 'simple.json'
        self.simple_schema_invalid_path = schema_path + 'simple_invalid.json'

    def test_read_raw_schema(self):
        raw_schema = Table(self.simple_schema_path, ST.big_query).raw_schema
        self.assertEqual(raw_schema, {"col_string":"STRING",
                                      "col_integer":"INTEGER",
                                      "col_float":"FLOAT",
                                      "col_date":"DATE",
                                      "col_datetime": "DATETIME",
                                      "col_boolean": "BOOLEAN"})

    def test_schema_validate_not_pass(self):
        with self.assertRaises(TypeError):
            schema = Table(self.simple_schema_invalid_path, ST.big_query)

    def test_read_schema(self):
        schema = Table(self.simple_schema_path, ST.big_query).schema
        self.assertEqual(schema, {"col_string": "string",
                                  "col_integer": "integer",
                                  "col_float": "double",
                                  "col_date": "date",
                                  "col_datetime": "timestamp",
                                  "col_boolean": "boolean"})

    def test_table_name(self):
        path = schema_path + 'default.test.json'
        table = Table(path, ST.big_query)
        table_name = table.name
        db_name = table.db_name
        self.assertEqual(table_name, "test")
        self.assertEqual(db_name, "default")

    def test_table_name_default(self):
        table = Table(self.simple_schema_path, ST.big_query)
        table_name = table.name
        db_name = table.db_name
        self.assertEqual(table_name, "simple")
        self.assertEqual(db_name, "default")
