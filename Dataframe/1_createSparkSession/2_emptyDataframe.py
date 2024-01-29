from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
sc=SparkSession.builder.appName("emptyDataframe").master("local[2]").getOrCreate()
empty_schema = StructType([])
df=sc.createDataFrame([],empty_schema)

df.show()

sc.stop()