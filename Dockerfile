FROM python:3.12-slim

ENV TZ=Europe/Kyiv

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY bot.py .
COPY heartbeat.py .
COPY models/ models/
COPY services/ services/
COPY monitors/ monitors/
COPY utils/ utils/

CMD ["python", "-u", "bot.py"]
