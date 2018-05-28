
from pyspark.sql import SparkSession


class HiveReader(object):

    def __init__(self, table=None):
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
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

    def select(self, db=None, table=None, condition=None, repair=False):
        """

        :rtype: pyspark.sql.DataFrame
        """
        if not db and self.db_name:
            db = self.db_name
        elif not db and not self.db_name:
            db = 'default'
        else:
            db = db

        if not table and self.table_name:
            table = self.table_name
        elif not table and not self.table_name:
            raise ValueError("Lack a table name to be selected")
        else:
            table = table

        if repair is True:
            self.spark.sql("* MSCK REPAIR TABLE `{}`.`{}`".format(db, table))

        if not condition:
            df = self.spark.sql("select * from `{}`.`{}`".format(db, table))
        elif condition and isinstance(condition, str):
            df = self.spark.sql("select * from `{}`.`{}` where {}".format(db, table, condition))
        else:
            raise ValueError("Partition key is not available")

        return df
