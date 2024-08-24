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

####################################################
es = Elasticsearch(hosts=elastic_node)

# Definisci il mapping
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

# Crea l'indice se non esiste
if not es.indices.exists(index=elastic_index):
    es.indices.create(index=elastic_index, body=mapping)
####################################################

sparkConf = SparkConf().set("es.nodes", "elasticsearch").set("es.port", "9200")

# Inizializza la SparkSession con supporto per Kafka
spark = SparkSession.builder \
    .appName("TeleSummary") \
    .config(conf=sparkConf) \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
#spark.sparkContext.setLogLevel("ERROR")

# Carica il tokenizer e il modello T5 pre-addestrato per il riassunto
tokenizer = T5Tokenizer.from_pretrained('t5-small')
model = T5ForConditionalGeneration.from_pretrained('t5-small')

#tokenizer = T5Tokenizer.from_pretrained('PRAli22/t5-base-text-summarizer')
#model = T5ForConditionalGeneration.from_pretrained('PRAli22/t5-base-text-summarizer')

def riassumi_testo(testo):
    # Prepara l'input per il modello T5
    if not testo:
        return "No text"
    
    input_text = "summarize: " + testo
    input_ids = tokenizer.encode(input_text, return_tensors='pt', max_length=512, truncation=True)
    
    # Genera il riassunto
    with torch.no_grad():
        summary_ids = model.generate(
            input_ids,
            max_length=150,
            min_length=10,
            length_penalty=2.0,
            num_beams=4,
            early_stopping=True
        )
    
    # Decodifica e restituisci il riassunto
    riassunto = tokenizer.decode(summary_ids[0], skip_special_tokens=True)
    return riassunto

# Crea una funzione UDF per Spark
riassumi_udf = udf(riassumi_testo, StringType())

#Funzione per debug
def write_batch_to_text(df, epoch_id):
    # Dataframe pandas
    pandas_df = df.toPandas()
    
    # Aggiunta in file di testo
    with open("/TeleSummary/spark/text_output.txt", "a") as f:
        for _, row in pandas_df.iterrows():
            line = f"id_message: {row['id_message']}, text: {row['text']}, sender: {row['sender']}, chat: {row['chat']}, id_chat: {row['id_chat']}, timestamp: {row['timestamp']}, Summary: {row['Summary']}\n"
            f.write(line)

# Configura la lettura del flusso dati da Kafka
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

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

# Decodifica dei dati dal formato Kafka
messages_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), data_schema).alias("messages_df")) \
        .select("messages_df.*")

# Applica la funzione di riassunto
df_riassunto = messages_df.withColumn("Summary", riassumi_udf(messages_df["text"])) \
        .withColumn("timestamp", date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd'T'HH:mm:ss.SSSX")) \
        .withColumn("text_length", length(col("text"))) \
        .withColumn("summary_length", length(col("Summary")))

#Mando dati ad elasticsearch
query_es = df_riassunto.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/es_checkpoint") \
    .option("es.resource", f"{elastic_index}") \
    .option("es.nodes", elastic_node) \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .start()

# Stampa il DataFrame su console in tempo reale
query_console = df_riassunto.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Scrittura Dataframe su text file per debugging
query_text = df_riassunto.writeStream \
    .foreachBatch(write_batch_to_text) \
    .outputMode("append") \
    .start()

# Attendi il termine del processo di streaming
query_es.awaitTermination()
query_console.awaitTermination()
