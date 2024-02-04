from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
file_path="F:\\SQLData\\employeeDepartment.csv"

sc=SparkSession.builder.appName("addRemoveColumns").master("local[2]").getOrCreate()
df=sc.read.csv(path=file_path,inferSchema=True,header=True)
df_new_col=df.withColumn("city",lit("Pune"))
df_new_col.show()