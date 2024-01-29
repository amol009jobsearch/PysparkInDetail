from pyspark.sql import SparkSession

sc=SparkSession.builder.appName("createDataframe").master("local[2]").getOrCreate()
emp_data=[(1,"Amol",30,40000),(2,"Amol",30,40000)]
emp_schema=["id","Name","Age","Salary"]
df=sc.createDataFrame(data=emp_data,schema=emp_schema)
df.show()
sc.stop()