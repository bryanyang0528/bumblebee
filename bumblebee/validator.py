
from pyspark.sql.functions import col


class Validator(object):
    def __init__(self):
        pass

    @classmethod
    def validate_data(cls, dataframe, schema: dict):
        return dataframe.select(*(col(c).cast(schema[c]) for c in dataframe.columns))