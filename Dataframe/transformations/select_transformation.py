from pyspark.sql import SparkSession

sc=SparkSession.builder.appName("select").master("local[2]").getOrCreate()
file_path="F:\\SQLData\\employeeDepartment.csv"

df=sc.read.csv(path=file_path,inferSchema=True,header=True)
df_select=df.select("empid","empname"," dept","salary")
df.printSchema()
df_select.show(10,False)
sc.stop()