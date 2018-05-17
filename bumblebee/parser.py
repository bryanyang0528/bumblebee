from __future__ import print_function, division
import json


class Parser(object):
    def __init__(self, schema_path, schema_type):
        with open(schema_path) as f:
            schema = json.load(f)
        validator = SchemaTypeValidator(schema_type)
        validator.validate_schema(schema)
        self.schema = schema


    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, new_schema):
        self._schema = new_schema


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

    big_query = ["STRING","BYTES","INTEGER","FLOAT","BOOLEAN","TIMESTAMP","DATE","DATETIME","RECORD"]
