import unittest

from bumblebee import Table

schema_path = 'tests/schema/'


class TestSchemaParser(unittest.TestCase):

    def setUp(self):
        self.simple_schema_path = schema_path + 'simple.json'
        self.simple_schema_invalid_path = schema_path + 'simple_invalid.json'
        self.schema_mapper = 'bq'

    def test_read_raw_schema(self):
        raw_schema = Table(self.simple_schema_path, self.schema_mapper).raw_schema
        self.assertEqual(raw_schema, {"col_string":"STRING",
                                      "col_integer":"INTEGER",
                                      "col_float":"FLOAT",
                                      "col_date":"DATE",
                                      "col_datetime": "DATETIME",
                                      "col_boolean": "BOOLEAN"})

    def test_schema_validate_not_pass(self):
        with self.assertRaises(TypeError):
            schema = Table(self.simple_schema_invalid_path, self.schema_mapper)

    def test_read_schema(self):
        schema = Table(self.simple_schema_path, self.schema_mapper).schema
        self.assertEqual(schema, {"col_string": "string",
                                  "col_integer": "integer",
                                  "col_float": "double",
                                  "col_date": "date",
                                  "col_datetime": "timestamp",
                                  "col_boolean": "boolean"})

    def test_table_name(self):
        path = schema_path + 'default.test.json'
        table = Table(path, self.schema_mapper)
        table_name = table.name
        db_name = table.db_name
        self.assertEqual(table_name, "test")
        self.assertEqual(db_name, "default")

    def test_table_name_default(self):
        table = Table(self.simple_schema_path, self.schema_mapper)
        table_name = table.name
        db_name = table.db_name
        self.assertEqual(table_name, "simple")
        self.assertEqual(db_name, "default")

    def test_table_name_error(self):
        path = schema_path + 'default.foo.test.json'
        with self.assertRaises(ValueError):
            schema = Table(path, self.schema_mapper)


class TestBQSchemaParser(unittest.TestCase):

    def setUp(self):
        self.bq_schema_path = schema_path + 'bq_dataset.bq_schema.json'
        self.bq_invalid_schema_path = schema_path + 'bq_dataset.bq_invalid_schema.json'
        self.schema_mapper = 'bq'

    def test_read_raw_schema(self):
        raw_schema = Table(self.bq_schema_path, self.schema_mapper, schema_parser='bq').raw_schema
        self.assertEqual(raw_schema, {"create_at": "STRING",
                                      "action_type": "STRING",
                                      "ad_id": "STRING",
                                      "cell_id": "STRING",
                                      "cost_points": "FLOAT"})

    def test_schema_validate_not_pass(self):
        with self.assertRaises(TypeError):
            raw_schema = Table(self.bq_invalid_schema_path, self.schema_mapper, schema_parser='bq').raw_schema