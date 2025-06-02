
import pyspark

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf()

conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "admin")
conf.set("spark.hadoop.fs.s3a.secret.key", "admin123" )
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

conf.set("spark.driver.memory", "2g")
conf.set("spark.jars", 
         "/E:/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,/E:/jars/kafka-clients-3.9.0.jar,/E:/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar,/E:/jars/commons-pool2-2.12.0.jar")

spark = SparkSession.builder.master("local[*]")\
                .config(conf=conf)\
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
                .appName("spark-stream")\
                .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("warn")


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

kafka = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "order") \
            .option("startingOffsets", "earliest") \
            .option("minOffsetsPerTrigger", "150") \
            .option("maxOffsetsPerTrigger", "400") \
            .option("failOnDataLoss", "false") \
            .load()

split_df = kafka.selectExpr("cast (value as string) as value") \
            .withColumn("data", from_json("value", schema)) \
            .withColumn("date", F.to_date("transactionTime")) \
            .withColumn("time", F.date_format("transactionTime", "HH:mm:ss")) \
            .select("data.*")
            # .withColumn("transaction", split(col("value"), "Transaction").cast("array<string>")) \
            # .select(element_at("transaction", 2).alias("Transaction1")) \
            # .drop(col("value")) \
            # .drop(col("transaction"))

# df = split_df.withColumn("data", from_json("Transaction1", schema=schema)) \
#             .select("data.*") \

split_df.printSchema()
            
            
df1 = split_df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", "s3a://spark-kafka/") \
            .option("checkpointLocation", "/E:/project/kafka-java-production/checkpoint") \
            .start()

df1.awaitTermination()
            