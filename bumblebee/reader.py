
from pyspark.sql import SparkSession


class HiveReader(object):

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    def select(self, db='default', table=None, condition=None):
        """

        :rtype: pyspark.sql.DataFrame
        """
        if not table:
            raise ValueError("Lack a table name to be selected")
        elif not condition:
            return self.spark.sql("select * from {}.{}".format(db, table))
        elif condition and isinstance(condition, str):
            df = self.spark.sql("select * from {}.{} where {}".format(db, table, condition))
            return df
        else:
            raise ValueError("Partition key is not available")
