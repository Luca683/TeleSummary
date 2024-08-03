from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Spark Session
spark = SparkSession.builder \
        .appName("TeleTranslate") \
        .getOrCreate()

#Read from kafka
kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.0.100.23:9092") \
        .option("subscribe", "msg_telegram") \
        .option("startingOffsets", "earliest") \
        .load()

#data_schema = StructType().add("id_message", IntegerType()).add("text", StringType()) \
#        .add("sender", MapType(keyType=StringType(), valueType=StringType())) \
#        .add("chat", StringType()) \
#        .add("id_chat", StringType()).add("timestamp", DoubleType())

data_schema = StructType([
    StructField("id_message", IntegerType()),
    StructField("text", StringType()),
    StructField("sender", StructType([
        StructField("username", StringType()),
        StructField("id_sender", LongType())
    ])),
    StructField("chat", StringType()),
    StructField("id_chat", LongType()),  # Cambiato a LongType
    StructField("timestamp", DoubleType())
])

data_received = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), data_schema).alias("data_received")) \
        .select("data_received.*")

query = data_received.writeStream \
        .outputMode("append") \
        .format("console") \
        .start() \
        .awaitTermination()

print("HELLO WORLD I'M USING SPARK!!!!")
