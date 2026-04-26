import os
import json
import logging
from flask import Flask, request

from handlers.thumbnail import process_thumbnail
from handlers.render import process_render
from handlers.optimize import process_optimize
from handlers.training import process_training

app = Flask(__name__)

# -----------------------------
# HEALTH CHECK (required by Cloud Run)
# -----------------------------
@app.route("/", methods=["GET"])
def health():
    return "Worker is running", 200


# -----------------------------
# PUBSUB PUSH ENDPOINT
# -----------------------------
@app.route("/pubsub", methods=["POST"])
def pubsub_handler():
    try:
        envelope = request.get_json()

        if not envelope:
            return ("No data", 400)

        message = envelope.get("message", {})
        data = message.get("data")

        import base64
        raw = base64.b64decode(data).decode("utf-8")

        logging.info(f"Received message: {raw}")

        params = json.loads(raw)
        msg_type = params.get("type")

        # -----------------------------
        # ROUTER (same logic you had)
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

        return ("OK", 200)

    except Exception as e:
        logging.exception("Worker error")
        return (str(e), 500)


# -----------------------------
# ENTRYPOINT (Cloud Run requires this)
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
