FROM python:3.12-slim-bullseye

COPY telegram_producer.py /app/telegram_producer.py
COPY requirements.txt ./

RUN pip install -r requirements.txt --break-system-packages

ENTRYPOINT ["python", "/app/telegram_producer.py"]