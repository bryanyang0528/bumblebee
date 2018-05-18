from __future__ import print_function, division
import json


class Table(object):
    def __init__(self, schema_path, schema_type: dict):
        with open(schema_path) as f:
            schema = json.load(f)
        self.schema_type = schema_type
        self.raw_schema = schema
        db_name, table_name = self.name_parser(schema_path)
        self.name = table_name
        self.db_name = db_name

    @property
    def raw_schema(self):
        return self._raw_schema

    @raw_schema.setter
    def raw_schema(self, raw_schema):
        validator = SchemaTypeValidator(self.schema_type.keys())
        validator.validate_schema(raw_schema)
        self.schema = self.map_schema(raw_schema, self.schema_type)
        self._raw_schema = raw_schema

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, new_schema):
        self._schema = new_schema

    def map_schema(self, raw_schema: dict, schema_type: dict):
        new_schema = {}
        for key, value in raw_schema.items():
            new_schema[key] = schema_type[value]

        return new_schema

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, db_name):
        self._db_name = db_name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @staticmethod
    def name_parser(path):
        filename = path.split("/")[-1]
        parts = filename.split(".")
        print(parts)
        if len(parts) == 3:
            return parts[0], parts[1]
        if len(parts) == 2:
            return "default", parts[0]
        else:
            raise ValueError("Filename should be [db_name].[table_name].json")

class SchemaTypeValidator(object):
    def __init__(self, valid_schema: list):
        self.valid_schema = valid_schema

    @property
    def valid_schema(self):
        return self._valid_schema

    @valid_schema.setter
    def valid_schema(self, valid_schema):
        self._valid_schema = valid_schema

    def validate_schema(self, schema: dict):
        """

        :param schema: {"col1":"STRING", "col2":"INTEGER"}
        :return: True or raise a TypeError
        """
        invalid_col = []
        for key, value in schema.items():
            if value not in self.valid_schema:
                invalid_col.append(key)

        if len(invalid_col) == 0:
            return True
        else:
            raise TypeError("Type of col '{}' is invalid".format(','.join(invalid_col)))


class SchemaTypes(object):

    big_query = {"STRING": "string",
                        "BYTES": "binary",
                        "INTEGER": "integer",
                        "FLOAT": "double",
                        "BOOLEAN" :"boolean",
                        "TIMESTAMP": "timestamp",
                        "DATE": "date",
                        "DATETIME": "timestamp",
                        "RECORD":"string"}
