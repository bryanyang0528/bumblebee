import attr

from bumblebee import Table, HiveReader, Validator
from bumblebee import SchemaMappers as SM


@attr.s
class Driver(object):
    src = attr.ib(default='hive')
    schema_path = attr.ib(default=attr.Factory(str))
    schema_mapper = attr.ib(default=attr.Factory(dict))
    _table = attr.ib(init=False)
    schema = attr.ib(init=False)
    reader = attr.ib(init=False)
    table_name = attr.ib(init=False)
    db_name = attr.ib(init=False)
    df = attr.ib(init=False)

    def __attrs_post_init__(self):
        self._table = Table(self.schema_path, self.schema_mapper)
        self.schema = self._table.schema
        self.reader = HiveReader(self._table)
        self.db_name = self.reader.db_name
        self.table_name = self.reader.table_name

    def read(self, condition=None):
        self.df = self.reader.select(condition=condition)
