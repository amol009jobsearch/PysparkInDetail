from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("PysparkTest").master("local[2]").getOrCreate()
emp_data=[(1,"Amol","Pune",1200),(2,"Tushar","Mumbai",2100)]
emp_schema=["Id","Name","City","Salary"]
df=sc.createDataFrame(data=emp_data,schema=emp_schema)
df.show()
sc.stop()