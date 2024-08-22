import os
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)
MY_TOKEN = os.environ.get("MY_TOKEN")

import asyncio
import time
from elasticsearch import Elasticsearch
from telegram.ext import Application

elastic_node = "http://10.0.100.25:9200"
elastic_index = "messages"

es = Elasticsearch(hosts=elastic_node) 

async def send_summary():
    application = Application.builder().token(MY_TOKEN).build()
    last_timestamp = 0

    while True:
        try:
            response = es.search(
                index=elastic_index,
                body={
                    "query": {
                        "range": {
                            "timestamp": {
                                "gt": last_timestamp
                            }
                        }
                    },
                    "sort": [{"timestamp": {"order": "asc"}}]
                }
            )

            for hit in response['hits']['hits']:
                message_summary = hit['_source']['Summary']
                await application.bot.send_message(chat_id=hit["_source"]["id_chat"], reply_to_message_id=hit["_source"]["id_message"], text=message_summary)
                last_timestamp = hit['_source']['timestamp']  # Aggiorna l'ultimo timestamp

            time.sleep(5)
        except Exception as e:
            continue

if __name__ == "__main__":
    asyncio.run(send_summary())