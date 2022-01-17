

#Huzaifa Kapasi 2021- MIT License

import os
scala_ver = '2.12'
spark_ver = '3.1.1'
spark_group = 'org.apache.spark'

packages = []
packages += f'{spark_group}:spark-sql-kafka-0-10_{scala_ver}:{spark_ver}'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages {} pyspark-shell'.format(','.join(packages))


from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType,ArrayType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col,struct,when,window
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd


sc = SparkContext('local')
spark = SparkSession(sc)

sc.setLogLevel("ERROR")

# Define the Schema

mySchema = StructType([ 
    StructField("Name",StringType(),False), 
    StructField("Class",IntegerType(),False), 
    StructField("GPA",FloatType(),False), 
    StructField("Occupation", StringType(), False) 
    
  ])
  
        
#Create the Streaming Dataframe

streamingDataFrame = spark.readStream.csv("/home/implied/Documents/streamReadDir/",header=True,schema=mySchema)
streamingDataFrame.printSchema()
#Publish the stream to Kafka

streamingDataFrame.selectExpr( "to_json(struct(*)) AS value").\
  writeStream \
  .format("kafka") \
  .option("topic", "csvdata") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("checkpointLocation", "/home/implied/Documents/chkpoint2_csv") \
  .start()
  
  
  
  
# Subscribe to topic to acccess the data
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "csvdata") \
  .load()
df.printSchema()




#df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), mySchema).alias("data")).select("data.*")



df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)") \
  .select(from_json(col("value"), mySchema).alias("data"), col("timestamp")) \
  .select("data.*", "timestamp")

df1.printSchema()

# root
 # |-- Name: string (nullable = true)
 # |-- Class: integer (nullable = true)
 # |-- GPA: float (nullable = true)
 # |-- Occupation: string (nullable = true)
 # |-- timestamp: timestamp (nullable = true)



# Group the data by window and word and compute the count of each group




df3=df1.select("Name").where(df1.GPA>3.5)



#See the data on console. You can push this data to another kafka topic as well or store in some file 
df3.writeStream \
    .format("console") \
    .option("truncate","false") \
    .outputMode('update') \
    .start() \
    .awaitTermination()
    
# +------------------------------------------+----------+-----+
# |window                                    |Occupation|count|
# +------------------------------------------+----------+-----+
# |{2021-04-20 16:41:00, 2021-04-20 16:42:00}|Founder   |1    |
# |{2021-04-20 16:41:00, 2021-04-20 16:42:00}|Architect |2    |
# |{2021-04-20 16:41:00, 2021-04-20 16:42:00}|Occupation|1    |
# |{2021-04-20 16:41:00, 2021-04-20 16:42:00}|Lawyer    |4    |
# |{2021-04-20 16:41:00, 2021-04-20 16:42:00}|Manager   |1    |
# +------------------------------------------+----------+-----+



  
  
  
