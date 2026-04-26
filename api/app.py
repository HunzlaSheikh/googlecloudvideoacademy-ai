from flask import Flask, request, jsonify
import logging

 
from pubsub_client import PubSubClient
from common.config import Config
# -----------------------------
# APP INIT
# -----------------------------
app = Flask(__name__)

# -----------------------------
# CLIENT INIT (reused)
# -----------------------------
pubsub_client = PubSubClient()


# -----------------------------
# HEALTH CHECK
# -----------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "environment": Config.ENVIRONMENT
    }), 200


# -----------------------------
# AI EVENT DETECT
# -----------------------------
@app.route("/ai-event-detect", methods=["POST"])
def ai_event_detect():
    try:
        data = request.get_json()

        if not data.get("videoId"):
            return jsonify({"error": "videoId is required"}), 400

        payload = {
            "type": "ai_event_detect",
            "videoId": data.get("videoId"),
            "projectId": data.get("projectId"),
            "startTime": data.get("startTime"),
            "endTime": data.get("endTime"),
            "chunk": data.get("chunk"),
            "domainId": data.get("domainId"),
            "time": data.get("time", "0")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "AI event detect queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# FINAL CLIP RENDER
# -----------------------------
@app.route("/final-clip-render", methods=["POST"])
def final_clip_render():
    try:
        data = request.get_json()

        if not data.get("projectID"):
            return jsonify({"error": "projectID is required"}), 400

        payload = {
            "type": "final_clip_render",
            "projectID": data.get("projectID"),
            "userID": data.get("userID"),
            "mergeType": data.get("mergeType"),
            "analysisId": data.get("analysisId"),
            "cameraAngle": data.get("cameraAngle")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "Final clip render queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# GENERATE THUMBNAIL
# -----------------------------
@app.route("/generate-thumbnail", methods=["POST"])
def generate_thumbnail():
    try:
        data = request.get_json()

        if not data.get("videoID"):
            return jsonify({"error": "videoID is required"}), 400

        payload = {
            "type": "generate_thumbnail",
            "videoID": data.get("videoID"),
            "videoType": data.get("videoType")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "Thumbnail queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# IMPORT CLIP TO LIBRARY
# -----------------------------
@app.route("/import-clip-to-library", methods=["POST"])
def import_clip_to_library():
    try:
        data = request.get_json()

        if not data.get("videoID"):
            return jsonify({"error": "videoID is required"}), 400

        payload = {
            "type": "import_clip_to_library",
            "videoID": data.get("videoID"),
            "libraryVideoID": data.get("libraryVideoID"),
            "clipID": data.get("clipID")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "Import clip queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# RETRAIN MODEL
# -----------------------------
@app.route("/retrain-model", methods=["POST"])
def retrain_model():
    try:
        data = request.get_json()

        if not data.get("retrain"):
            return jsonify({"error": "retrain is required"}), 400

        payload = {
            "type": "retrain_model",
            "retrain": data.get("retrain"),
            "version": data.get("version"),
            "domainId": data.get("domainId")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "Model training queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# RENDER CLIP
# -----------------------------
@app.route("/render-clip", methods=["POST"])
def render_clip():
    try:
        data = request.get_json()

        if not data.get("videoID"):
            return jsonify({"error": "videoID is required"}), 400

        payload = {
            "type": "render_clip",
            "videoID": data.get("videoID"),
            "renderType": data.get("renderType"),
            "userID": data.get("userID"),
            "analysisId": data.get("analysisId")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "Render clip queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# ICLIP RENDER
# -----------------------------
@app.route("/iclip-render", methods=["POST"])
def iclip_render():
    try:
        data = request.get_json()

        if not data.get("videoID"):
            return jsonify({"error": "videoID is required"}), 400

        payload = {
            "type": "iclip_render",
            "videoID": data.get("videoID"),
            "clipID": data.get("clipID"),
            "userID": data.get("userID")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "iClip render queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# OPTIMIZE VIDEO
# -----------------------------
@app.route("/optimize-video", methods=["POST"])
def optimize_video():
    try:
        data = request.get_json()

        if not data.get("videoID"):
            return jsonify({"error": "videoID is required"}), 400

        payload = {
            "type": "optimize_video",
            "videoID": data.get("videoID")
        }

        message_id = pubsub_client.publish(payload)

        return jsonify({
            "message": "Optimize video queued",
            "message_id": message_id
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# RUN APP (Cloud Run entry)
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(Config.PORT) if hasattr(Config, "PORT") else 8080)