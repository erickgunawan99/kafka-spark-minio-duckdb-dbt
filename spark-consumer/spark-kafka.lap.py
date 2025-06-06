import findspark
findspark.init()
import pyspark

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

conf = SparkConf()

conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "PgceeJEz8SoSvEXJ6voQ")
conf.set("spark.hadoop.fs.s3a.secret.key", "7aSUqHuC7VnTItnVTHeIAuDPr4hdG6Q7rxaR1eVl" )
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")


conf.set("spark.driver.memory", "2g")
conf.set("spark.jars", 
         "/C:/Users/Files/New folder/jars/spark-kafka-3.4.4/spark-sql-kafka-0-10_2.12-3.4.4.jar,/C:/Users/Files/New folder/jars/spark-kafka-3.4.4/kafka-clients-3.3.2.jar,/C:/Users/Files/New folder/jars/spark-kafka-3.4.4/spark-token-provider-kafka-0-10_2.12-3.4.4.jar,/C:/Users/Files/New folder/jars/spark-kafka-3.4.4/commons-pool2-2.11.1.jar")

spark = SparkSession.builder.master("local[2]")\
                .config(conf=conf)\
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
                .appName("spark-stream")\
                .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("warn")

# data =  {"transactionId": "530e277d-585b-4198-8e58-4fe80da6ca8f", "merchantId": 162, 
#          "product": "sony dslr", "quantity": 4, "customerId": 2882, "amount": 140.26793137241745, 
#          "transactionTime": "2025-05-30 00:22:46", "paymentType": "credit_card"} 

# pd_df = pd.DataFrame(data)



schema = StructType([  
    StructField("transactionId", StringType(), True),
    StructField("merchantId", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("customerId", IntegerType(), True),
    StructField("amount", FloatType(), True),
    StructField("transactionTime", TimestampType(), True),
    StructField("paymentType", StringType(), True)
])

# test = spark.createDataFrame(list_data, schema)

# test1 = test.withColumn("date", F.to_date("transactionTime")) \
#             .withColumn("time", F.date_format("transactionTime", "HH:mm:ss"))



kafka = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "order") \
            .option("startingOffsets", "earliest") \
            .option("auto.offset.reset", "earliest") \
            .option("minOffsetsPerTrigger", "150") \
            .option("maxOffsetsPerTrigger", "400") \
            .option("failOnDataLoss", "false") \
            .load()

df = kafka.selectExpr("cast (value as string) as value") \
            .withColumn("data", from_json("value", schema)) \
            .withColumn("date", F.to_date("data.transactionTime")) \
            .withColumn("time", F.date_format("data.transactionTime", "HH:mm:ss")) \
            .select("data.*", "date", "time") \
#             # .withColumn("transaction", split(col("value"), "Transaction").cast("array<string>")) \
#             # .select(element_at("transaction", 2).alias("Transaction1")) \
#             # .drop(col("value")) \
#             # .drop(col("transaction"))

# # df = split_df.withColumn("data", from_json("Transaction1", schema=schema)) \
# #             .select("data.*") \

df.printSchema()
            
            
stream_df = df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .partitionBy("time") \
            .option("path", "s3a://spark-kafka-source/") \
            .option("checkpointLocation", "/C:/Users/Files/project/kafka-java-production/spark-consumer/checkpoint") \
            .start()

stream_df.awaitTermination()
            