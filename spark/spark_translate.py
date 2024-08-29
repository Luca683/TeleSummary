from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from transformers import T5Tokenizer, T5ForConditionalGeneration
from elasticsearch import Elasticsearch
import torch

kafka_broker = "10.0.100.23:9092"
elastic_node = "http://10.0.100.25:9200/"
elastic_index = "messages"
input_topic = "msg_telegram"

# Initialize the elasticsearch client
es = Elasticsearch(hosts=elastic_node)

# Define the elasticsearch index mapping
mapping = {
    "settings": {
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords": "_english_"  
                }
            },
            "analyzer": {
                "custom_english": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop"
                    ]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "text": {
                "type": "text",
                "analyzer": "custom_english",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                },
                "fielddata": True
            },
            "Summary": {
                "type": "text",
                "analyzer": "custom_english",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                },
                "fielddata": True
            },
            "id_message": {
                "type": "integer"
            },
            "sender": {
                "properties": {
                    "username": {
                        "type": "keyword"
                    },
                    "id_sender": {
                        "type": "long"
                    }
                }
            },
            "chat": {
                "type": "keyword"
            },
            "id_chat": {
                "type": "long"
            },
            "timestamp": {
                "type": "date"
            },
            "text_length": {
                "type": "integer"
            },
            "summary_length": {
                "type": "integer"
            }
        }
    }
}

# If not exist: create the elasticsearch index
if not es.indices.exists(index=elastic_index):
    es.indices.create(index=elastic_index, body=mapping)

# Spark configuration with elasticsearch connection settings
sparkConf = SparkConf().set("es.nodes", "elasticsearch").set("es.port", "9200")

# SparkSession with kafka support
spark = SparkSession.builder \
    .appName("TeleSummary") \
    .config(conf=sparkConf) \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
#spark.sparkContext.setLogLevel("ERROR")

# Load the pre-trained T5 tokenizer and model for text summarization
tokenizer = T5Tokenizer.from_pretrained('t5-small')
model = T5ForConditionalGeneration.from_pretrained('t5-small')

#tokenizer = T5Tokenizer.from_pretrained('PRAli22/t5-base-text-summarizer')
#model = T5ForConditionalGeneration.from_pretrained('PRAli22/t5-base-text-summarizer')

# Function to summarize text using the T5 model
def text_summarization(text):
    if not text:
        return "No text"
    
    # Tokenize the input text
    input_text = "summarize: " + text
    input_ids = tokenizer.encode(input_text, return_tensors='pt', max_length=512, truncation=True)
    
    # Generate summary
    with torch.no_grad():
        summary_ids = model.generate(
            input_ids,
            max_length=150,
            min_length=10,
            length_penalty=2.0,
            num_beams=4,
            early_stopping=True
        )
    
    # Decode into a readable summary
    summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True)
    return summary

# Create a UDF for Spark
riassumi_udf = udf(text_summarization, StringType())

# Debugging function to write each batch of processed data to a text file
def write_batch_to_text(df, epoch_id):
    # Dataframe pandas
    pandas_df = df.toPandas()
    
    # Append data to a text file
    with open("/TeleSummary/spark/text_output.txt", "a") as f:
        for _, row in pandas_df.iterrows():
            line = f"id_message: {row['id_message']}, text: {row['text']}, sender: {row['sender']}, chat: {row['chat']}, id_chat: {row['id_chat']}, timestamp: {row['timestamp']}, Summary: {row['Summary']}\n"
            f.write(line)

# Configure the streaming data source from kafka
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema for the incoming kafka data
data_schema = StructType([
    StructField("id_message", IntegerType()),
    StructField("text", StringType()),
    StructField("sender", StructType([
        StructField("username", StringType()),
        StructField("id_sender", LongType())
    ])),
    StructField("chat", StringType()),
    StructField("id_chat", LongType()),
    StructField("timestamp", DoubleType())
])

# Decode the data from kafka format
messages_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), data_schema).alias("messages_df")) \
        .select("messages_df.*")

# Apply the summarization function to the data, modify timestamp format, and add text length e summary length
df_riassunto = messages_df.withColumn("Summary", riassumi_udf(messages_df["text"])) \
        .withColumn("timestamp", date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd'T'HH:mm:ss.SSSX")) \
        .withColumn("text_length", length(col("text"))) \
        .withColumn("summary_length", length(col("Summary")))

# Write data to elasticsearch
query_es = df_riassunto.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/es_checkpoint") \
    .option("es.resource", f"{elastic_index}") \
    .option("es.nodes", elastic_node) \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .start()

# Debugging: print DataFrame to the console in real-time
query_console = df_riassunto.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Debugging: write DataFrame to a text file 
query_text = df_riassunto.writeStream \
    .foreachBatch(write_batch_to_text) \
    .outputMode("append") \
    .start()

# Wait for the streaming queries to finish
query_es.awaitTermination()
query_console.awaitTermination()
