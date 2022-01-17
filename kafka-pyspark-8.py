
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
  .option("includeHeaders'", "false") \
  .load()
df.printSchema()



#df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), mySchema).alias("data")).select("data.*")



df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)" ) \
  .select(from_json(col("value"), mySchema).alias("data"), col("timestamp")) \
  .select("data.*", "timestamp")

df1.printSchema()

# root
 # |-- Name: string (nullable = true)
 # |-- Class: integer (nullable = true)
 # |-- GPA: float (nullable = true)
 # |-- Occupation: string (nullable = true)
 # |-- timestamp: timestamp (nullable = true)
 
df1.writeStream \
    .format("console") \
    .option("truncate","false") \
    .outputMode('update') \
    .start() \
    
    # +---------------+-----+----+----------+-----------------------+
# |Name           |Class|GPA |Occupation|timestamp              |
# +---------------+-----+----+----------+-----------------------+
# |Name           |null |null|Occupation|2021-04-23 15:34:08.955|
# |Sally Whittaker|2018 |3.75|Manager   |2021-04-23 15:34:09.058|
# |Belinda Jameson|2017 |3.52|Founder   |2021-04-23 15:34:09.059|
# |Jeff Smith     |2018 |3.2 |Architect |2021-04-23 15:34:09.06 |
# |Sandy Allen    |2019 |3.48|Lawyer    |2021-04-23 15:34:09.061|
# |Sandy Allen    |2019 |3.48|Lawyer    |2021-04-23 15:34:09.062|
# |Jeff Smith     |2018 |3.2 |Architect |2021-04-23 15:34:09.063|
# |Sandy Allen    |2019 |3.48|Lawyer    |2021-04-23 15:34:09.064|
# |Sandy Allen    |2019 |3.48|Lawyer    |2021-04-23 15:34:09.065|
# +---------------+-----+----+----------+-----------------------+

    


# Group the data by window and word and compute the count of each group

df4= df1.withWatermark("timestamp", "1 minutes").groupBy(window(df1.timestamp,  "10 minutes","5 minutes"),df1.Occupation).count()

#See the data on console. You can push this data to another kafka topic as well or store in some file 
df4.writeStream \
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
  
