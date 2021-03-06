from __future__ import print_function, division
import json

class Table(object):
    def __init__(self, schema_path, schema_mapper: str, schema_parser: str='simple'):
        """
        
        :param schema_path: 
        :param schema_mapper:  a dict. key: valid data type of target db(like BQ);
                                       value: valid data type of SparkSQL

        """

        self.schema_mapper = SchemaMappers.get_mapper(schema_mapper)
        self.raw_schema = self.schema_parser(schema_path, self.schema_mapper, schema_parser)
        self.schema = self.map_schema(self.raw_schema, self.schema_mapper)
        db_name, table_name = self.name_parser(schema_path)
        self.name = table_name
        self.db_name = db_name

    @property
    def raw_schema(self):
        return self._raw_schema

    @raw_schema.setter
    def raw_schema(self, raw_schema):
        self._raw_schema = raw_schema

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, new_schema):
        self._schema = new_schema

    @staticmethod
    def map_schema(raw_schema: dict, schema_type: dict):
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
        if len(parts) == 3:
            return parts[0], parts[1]
        if len(parts) == 2:
            return "default", parts[0]
        else:
            raise ValueError("Filename should be [db_name].[table_name].json")

    @staticmethod
    def schema_parser(schema_path, schema_mapper, schema_parser):
        with open(schema_path) as f:
            data = json.load(f)

        if schema_parser == 'simple':
            schema = data
        elif schema_parser == 'bq':
            schema = {}
            for d in data:
                schema[d['name']] = d['type']
        else:
            raise ValueError('Did not support this schema_parser {}.'.format(schema_parser))

        validator = SchemaTypeValidator(schema_mapper.keys())
        validator.validate_schema(schema)
        return schema


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

        :param schema: schema from the source json file
                       {"col1":"STRING", "col2":"INTEGER"}
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


class SchemaMappers(object):
    """
    Schema Mappers maps the schema between target db and spark
    """

    _big_query = {"STRING": "string",
                        "BYTES": "binary",
                        "INTEGER": "integer",
                        "FLOAT": "double",
                        "BOOLEAN" :"boolean",
                        "TIMESTAMP": "timestamp",
                        "DATE": "date",
                        "DATETIME": "timestamp",
                        "RECORD":"string"}

    @classmethod
    def get_mapper(cls, mapper):
        if mapper == 'bq':
            return cls._big_query
        else:
            raise ValueError('Did not support this mapper: {}.'.format(mapper))