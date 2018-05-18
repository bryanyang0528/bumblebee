
from pyspark.sql import SparkSession


class HiveReader(object):

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    def select(self, db='default', table=None, partition=None):
        if not table:
            raise ValueError("Lack a table name to be selected")
        elif not partition:
            return self.spark.sql("select * from {}.{}".format(db, table))
        elif isinstance(partition, dict) and len(partition.keys()) == 1:
            for filed, condition in partition.items():
                df = self.spark.sql("select * from {}.{} where {} {}".format(db, table, filed, condition))
                return df
        else:
            raise ValueError("Partition key is not available")

