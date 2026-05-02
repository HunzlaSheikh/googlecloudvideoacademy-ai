from flask import Flask, request, jsonify
import logging
import json
import requests
import google.auth
from google.auth.transport.requests import Request

from common.config import Config

app = Flask(__name__)

PROJECT_ID = Config.GCP_PROJECT
REGION = Config.GCP_REGION

# -----------------------------
# AUTH (cached)
# -----------------------------
credentials, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

def get_token():
    if not credentials.valid:
        credentials.refresh(Request())
    return credentials.token


# -----------------------------
# JOB TRIGGER
# -----------------------------
def trigger_job(job_name, payload):
    try:
        url = f"https://run.googleapis.com/v2/projects/{PROJECT_ID}/locations/{REGION}/jobs/{job_name}:run"

        body = {
            "overrides": {
                "containerOverrides": [
                    {
                        "env": [
                            {
                                "name": "JOB_PAYLOAD",
                                "value": json.dumps(payload)
                            }
                        ]
                    }
                ]
            }
        }

        headers = {
            "Authorization": f"Bearer {get_token()}",
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, json=body, timeout=10)

        if response.status_code not in [200, 201, 202]:
            logging.error(f"Job trigger failed: {response.text}")
            raise Exception(response.text)

        try:
            return response.json()
        except:
            return {"status": "started"}

    except requests.exceptions.Timeout:
        # VERY IMPORTANT: job might still be running
        logging.warning("Job trigger timeout - assuming started")
        return {"status": "started"}

    except Exception as e:
        logging.exception("Error triggering Cloud Run Job")
        raise


# -----------------------------
# GENERIC HANDLER
# -----------------------------
def handle_request(required_field, payload_builder, job_name, success_msg):
    try:
        data = request.get_json()

        if not data or not data.get(required_field):
            return jsonify({"error": f"{required_field} is required"}), 400

        payload = payload_builder(data)

        job = trigger_job(job_name, payload)

        return jsonify({
            "message": success_msg,
            "job": job
        }), 200

    except Exception as e:
        logging.exception(e)
        return jsonify({"error": str(e)}), 500


# -----------------------------
# HEALTH
# -----------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "environment": Config.ENVIRONMENT
    }), 200


# -----------------------------
# ENDPOINTS
# -----------------------------
@app.route("/ai-event-detect", methods=["POST"])
def ai_event_detect():
    return handle_request(
        "videoId",
        lambda d: {
            "type": "ai_event_detect",
            "videoId": d.get("videoId"),
            "projectId": d.get("projectId"),
            "startTime": d.get("startTime"),
            "endTime": d.get("endTime"),
            "chunk": d.get("chunk"),
            "domainId": d.get("domainId"),
            "time": d.get("time", "0")
        },
        "ai-event-detect-job",
        "AI event detection started"
    )


@app.route("/final-clip-render", methods=["POST"])
def final_clip_render():
    return handle_request(
        "projectID",
        lambda d: {
            "type": "final_clip_render",
            "projectID": d.get("projectID"),
            "userID": d.get("userID"),
            "mergeType": d.get("mergeType"),
            "analysisId": d.get("analysisId"),
            "cameraAngle": d.get("cameraAngle")
        },
        "final-clip-job",
        "Final clip render started"
    )


@app.route("/generate-thumbnail", methods=["POST"])
def generate_thumbnail():
    return handle_request(
        "videoID",
        lambda d: {
            "type": "generate_thumbnail",
            "videoID": d.get("videoID"),
            "videoType": d.get("videoType")
        },
        "video-render-job-staging",  
        "Thumbnail job started"
    )


@app.route("/render-clip", methods=["POST"])
def render_clip():
    return handle_request(
        "videoID",
        lambda d: {
            "type": "render_clip",
            "videoID": d.get("videoID"),
            "renderType": d.get("renderType"),
            "userID": d.get("userID"),
            "analysisId": d.get("analysisId")
        },
        "video-render-job-staging",
        "Render job started"
    )


@app.route("/optimize-video", methods=["POST"])
def optimize_video():
    return handle_request(
        "videoID",
        lambda d: {
            "type": "optimize_video",
            "videoID": d.get("videoID")
        },
        "video-render-job-staging",
        "Optimize job started"
    )


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(getattr(Config, "PORT", 8080)))