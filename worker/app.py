import os
import json
import logging
from flask import Flask, request

from handlers.thumbnails import process_thumbnail
# from handlers.render import process_render
# from handlers.optimize import process_optimize
# from handlers.training import process_training

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/pubsub", methods=["POST"])
def pubsub_handler():
    try:
        envelope = request.get_json(silent=True)

        if not envelope:
            return ("No message", 400)

        message = envelope.get("message", {})
        data = message.get("data")

        import base64
        raw = base64.b64decode(data).decode("utf-8")

        logging.info(f"Message: {raw}")

        params = json.loads(raw)
        msg_type = params.get("type")

        if msg_type == "generate_thumbnail":
            process_thumbnail(params)
        # elif msg_type == "render_clip":
        #     process_render(params)
        # elif msg_type == "optimize_video":
        #     process_optimize(params)
        # elif msg_type == "retrain_model":
        #     process_training(params)

        return ("OK", 200)

    except Exception as e:
        logging.exception(e)
        return (str(e), 500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
