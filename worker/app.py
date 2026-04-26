import os
import json
import logging
from google.cloud import pubsub_v1

from handlers.thumbnail import process_thumbnail
from handlers.render import process_render
from handlers.optimize import process_optimize
from handlers.training import process_training

# -----------------------------
# CONFIG
# -----------------------------
PROJECT_ID = os.environ["GCP_PROJECT"]
SUBSCRIPTION_ID = os.environ["PUBSUB_SUBSCRIPTION"]

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)


# -----------------------------
# MESSAGE HANDLER
# -----------------------------
def callback(message):

    try:
        raw = message.data.decode("utf-8")
        logging.info(f"Received message: {raw}")

        params = json.loads(raw)
        msg_type = params.get("type")

        # -----------------------------
        # ROUTER (replaces Azure Queue switch logic)
        # -----------------------------
        if msg_type == "generate_thumbnail":
            process_thumbnail(params)

        elif msg_type == "render_clip":
            process_render(params)

        elif msg_type == "optimize_video":
            process_optimize(params)

        elif msg_type == "retrain_model":
            process_training(params)

        else:
            logging.warning(f"Unknown type: {msg_type}")

        message.ack()

    except Exception as e:
        logging.exception("Worker error")
        message.nack()


# -----------------------------
# START LISTENER
# -----------------------------
def start_worker():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logging.info("Worker started... listening for Pub/Sub messages")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()


if __name__ == "__main__":
    start_worker()