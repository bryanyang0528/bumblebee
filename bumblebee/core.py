import logging

import attr

from bumblebee import Table, HiveReader, Validator

logger = logging.getLogger(__name__)


@attr.s
class Driver(object):
    src = attr.ib(default='hive')
    schema_path = attr.ib(default=attr.Factory(str))
    schema_mapper = attr.ib(default=attr.Factory(dict))
    schema_parser = attr.ib(default='simple')
    _table = attr.ib(init=False)
    schema = attr.ib(init=False)
    reader = attr.ib(init=False)
    table_name = attr.ib(init=False)
    db_name = attr.ib(init=False)
    _df = attr.ib(init=False, default=None)
    _valid_df = attr.ib(init=False, default=None)
    _sum_df = attr.ib(init=False, default=None)

    def __attrs_post_init__(self):
        self._table = Table(self.schema_path, self.schema_mapper, self.schema_parser)
        self.schema = self._table.schema
        self.reader = HiveReader(self._table)
        self.db_name = self.reader.db_name
        self.table_name = self.reader.table_name

    @property
    def df(self):
        if not self._df:
            raise ValueError("df is not ready, please 'read' it first.")
        else:
            return self._df

    @property
    def valid_df(self):
        if not self._valid_df:
            raise ValueError("valid df is not ready, please 'validate' it first.")
        else:
            return self._valid_df

    @property
    def sum_df(self):
        if not self._sum_df:
            raise ValueError("Please check sum first.")
        else:
            return self._sum_df

    def read(self, condition=None, repair=False):
        self._df = self.reader.select(condition=condition, repair=repair)
        return self

    def validate(self, validate_schema=True, rule='bq'):
        self._valid_df = Validator.validate_data(self.df, self.schema)

        if validate_schema is True:
            self._valid_df = Validator.validate_schema(self._valid_df, rule=rule)

        return self

    def check_sum(self):
        if self.df and self.valid_df:
            sum_df = self.df.count()
            sum_valid_df = self.valid_df.count()
            result = sum_df == sum_valid_df

            if result is True:
                logger.info("Check Sum Passed! Result: {}".format(sum_valid_df))
                self._sum_df = sum_df
                return True
            else:
                logger.error("Check Sum Failed! df: {}, valid_df: {}".format(sum_df, sum_valid_df))
                raise ValueError("Check Sum Failed!")

    def write(self, file_format, path_prefix):
        full_path = '{}/{}/{}'.format(path_prefix, self.db_name, self.table_name)
        self._valid_df.write.format(file_format).save(full_path)
        return True
