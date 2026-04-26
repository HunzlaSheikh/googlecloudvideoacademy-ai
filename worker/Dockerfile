FROM python:3.10

WORKDIR /app

# system deps (needed for moviepy / ffmpeg)
RUN apt-get update && apt-get install -y ffmpeg

# copy entire project (so common + worker + api all accessible if needed)
COPY . /app

# install dependencies 
RUN pip install --no-cache-dir -r requirements.txt
# ensure Python can find shared modules
ENV PYTHONPATH=/app

# start worker
CMD ["python", "worker/app.py"]
