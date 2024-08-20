from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transformers import T5Tokenizer, T5ForConditionalGeneration
import torch

kafka_broker = "10.0.100.23:9092"
input_topic = "msg_telegram"

# Inizializza la SparkSession con supporto per Kafka
spark = SparkSession.builder \
    .appName("RiassuntoTestoKafkaStreaming") \
    .getOrCreate()

# Carica il tokenizer e il modello T5 pre-addestrato per il riassunto
tokenizer = T5Tokenizer.from_pretrained('t5-small')
model = T5ForConditionalGeneration.from_pretrained('t5-small')

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
            min_length=50,
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
    with open("/TeleTranslate/spark/text_output.txt", "a") as f:
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
df_riassunto = messages_df.withColumn("Summary", riassumi_udf(messages_df["text"]))

# Stampa il DataFrame in tempo reale
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
query_console.awaitTermination()
