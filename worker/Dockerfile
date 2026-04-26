FROM python:3.10

WORKDIR /app

RUN apt-get update && apt-get install -y ffmpeg

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["gunicorn", "-b", "0.0.0.0:8080", "worker.app:app"]
