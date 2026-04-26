import json
import logging
from google.cloud import pubsub_v1
from common.config import Config


class PubSubClient:
    """
    Central Pub/Sub publisher for Cloud Run API service.
    """

    def __init__(self):
        # -----------------------------
        # CONFIG (centralized)
        # -----------------------------
        self.project_id = Config.GCP_PROJECT
        self.topic_id = Config.PUBSUB_TOPIC

        # -----------------------------
        # PUBSUB CLIENT
        # -----------------------------
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(
            self.project_id,
            self.topic_id
        )

    # -----------------------------
    # PUBLISH METHOD
    # -----------------------------
    def publish(self, payload: dict) -> str:
        """
        Publishes a message to Pub/Sub and returns message ID.
        """

        try:
            message_bytes = json.dumps(payload).encode("utf-8")

            future = self.publisher.publish(self.topic_path, message_bytes)
            message_id = future.result()

            logging.info(f"Published message ID: {message_id}")

            return message_id

        except Exception as e:
            logging.exception("Failed to publish Pub/Sub message")
            raise