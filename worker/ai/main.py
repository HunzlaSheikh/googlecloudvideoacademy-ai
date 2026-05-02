import os
import json
import logging

from handlers.thumbnails import process_thumbnail
# from handlers.aieventdetect import process_event_detect

def main():
    try:
        # Cloud Run Job passes args via ENV or file
        payload = os.environ.get("JOB_PAYLOAD")

        if not payload:
            raise Exception("No JOB_PAYLOAD provided")

        params = json.loads(payload)

        msg_type = params.get("type")

        logging.info(f"Running job: {msg_type}")

        if msg_type == "generate_thumbnail":
            process_thumbnail(params)

        # elif msg_type == "event_detect":
        #     process_event_detect(params)

        logging.info("Job completed successfully")

    except Exception as e:
        logging.exception("Job failed")
        raise e


if __name__ == "__main__":
    main()