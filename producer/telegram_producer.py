import os
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)
MY_TOKEN = os.environ.get("MY_TOKEN")

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

from telegram import Update
from telegram.ext import MessageHandler, Application, ContextTypes


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.text is not None:
        msg_to_logstash = {
            "id_message": msg.message_id,
            "text": msg.text,
            "sender":{
                "username": msg.from_user.username,
                "id_sender": msg.from_user.id
            },
            "chat": msg.chat.effective_name,
            "id_chat": msg.chat.id,
            "timestamp": msg.date.timestamp()
        }
    
    print(msg_to_logstash)
    logger.info(f"Message received: {msg_to_logstash}")

def main() -> None:
    application = Application.builder().token(MY_TOKEN).build()
    message_handler = MessageHandler(None, handle_message)
    application.add_handler(message_handler)
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
