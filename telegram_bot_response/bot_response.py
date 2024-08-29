# Retrive Telegram bot token
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

# Initialize the elasticsearch client
es = Elasticsearch(hosts=elastic_node) 

# Function to send message summary to the telegram group
async def send_summary_to_group():
    application = Application.builder().token(MY_TOKEN).build()

    # Start from the earliest possible timestamp.
    last_timestamp = "1970-01-01T00:00:00.000Z"

    # Continuously fetch new summaries and send them to the appropriate chat.
    while True:
        try:
             # Query Elasticsearch for messages with a timestamp greater than the last retrieved timestamp,
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

            # Iterate through the search results.
            for hit in response['hits']['hits']:
                # Extract the summary of the message from the Elasticsearch document.
                message_summary = hit['_source']['Summary']

                 # Send the summary as a reply to the original message in the appropriate chat.
                await application.bot.send_message(
                    chat_id=hit["_source"]["id_chat"],
                    reply_to_message_id=hit["_source"]["id_message"], 
                    text=message_summary)
                
                # Update the last_timestamp
                last_timestamp = hit['_source']['timestamp']  # Aggiorna l'ultimo timestamp

            time.sleep(5)
        except Exception as e:
            continue

if __name__ == "__main__":
    asyncio.run(send_summary_to_group())