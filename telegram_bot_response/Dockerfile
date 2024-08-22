FROM python:3.12-slim-bullseye

COPY bot_response.py /app/bot_response.py
COPY requirements.txt ./

RUN pip install -r requirements.txt --break-system-packages

ENTRYPOINT ["python", "/app/bot_response.py"]