
from pyspark.sql import SparkSession


class HiveReader(object):

    def __init__(self, table=None):
        self.spark = SparkSession.builder.getOrCreate()
        if table:
            self.db_name = table.db_name
            self.table_name = table.name
        else:
            self.db_name = None
            self.table_name = None

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, db_name):
        self._db_name = db_name

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, table_name):
        self._table_name = table_name

    def select(self, db='default', table=None, condition=None):
        """

        :rtype: pyspark.sql.DataFrame
        """
        if not table and not self.table_name:
            raise ValueError("Lack a table name to be selected")
        elif not condition:
            return self.spark.sql("select * from {}.{}".format(db, table))
        elif condition and isinstance(condition, str):
            df = self.spark.sql("select * from {}.{} where {}".format(db, table, condition))
            return df
        else:
            raise ValueError("Partition key is not available")
