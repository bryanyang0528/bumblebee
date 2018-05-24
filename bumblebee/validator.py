import re

from pyspark.sql.functions import col


class Validator(object):
    def __init__(self):
        pass

    @classmethod
    def validate_data(cls, dataframe, schema: dict):
        return dataframe.select(*(col(c).cast(schema[c]) for c in dataframe.columns))

    @classmethod
    def validate_schema(cls, dataframe, rule: str='bq'):
        if rule == 'bq':
            new_column_name_list = ['_' + x if re.match(r'[^\D]', x) else x for x in dataframe.columns]
        else:
            raise ValueError('Did not support this rule: {}'.format(rule))

        dataframe = dataframe.toDF(*new_column_name_list)
        return dataframe