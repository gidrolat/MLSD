FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r /app/requirements.txt \
    && python -m nltk.downloader vader_lexicon

CMD ["python", "predict_service.py"]