import logging
import pathlib
import shutil
import subprocess
import json

from google.cloud import storage

from common import dbintegeration as new_db
from common.config import Config


cfg = Config()
storage_client = storage.Client()


# =========================
# MAIN HANDLER
# =========================
def process_thumbnail(params):

    projectID = 0
    videoID = params.get("videoID")
    videoType = params.get("videoType")

    try:
        logging.info(f"Thumbnail job started: videoID={videoID}")

        new_db.InsertLogs(
            0, videoID,
            "thumbnail",
            "process_thumbnail",
            "start"
        )

        base_path = pathlib.Path("/tmp") / str(videoID)
        base_path.mkdir(parents=True, exist_ok=True)

        # =====================================================
        # PROJECT VIDEO
        # =====================================================
        if videoType == "projectVideo":

            videoUrls = new_db.GetProjectVideoUrls(videoID)

            for item in videoUrls:

                videoUrl = item.Url
                projectID = item.ProjectId

                try:
                    new_db.InsertLogs(projectID, videoID, "thumbnail", "start", "start")

                    thumbnail_name = f"VideoThumbnail_{videoID}_{projectID}.jpg"
                    thumbnail_path = base_path / thumbnail_name

                    # -----------------------------
                    # PURE FFMPEG (NO MOVIEPY)
                    # -----------------------------
                    cmd = [
                        "ffmpeg",
                        "-y",
                        "-ss", "1",
                        "-i", videoUrl,
                        "-frames:v", "1",
                        "-q:v", "2",
                        str(thumbnail_path)
                    ]

                    subprocess.run(cmd, check=True)

                    # -----------------------------
                    # METADATA (ffprobe)
                    # -----------------------------
                    probe_cmd = [
                        "ffprobe",
                        "-v", "error",
                        "-print_format", "json",
                        "-show_format",
                        "-show_streams",
                        videoUrl
                    ]

                    result = subprocess.run(
                        probe_cmd,
                        capture_output=True,
                        text=True,
                        check=True
                    )

                    data = json.loads(result.stdout)

                    video_stream = next(
                        (s for s in data["streams"] if s["codec_type"] == "video"),
                        None
                    )

                    width = video_stream.get("width")
                    height = video_stream.get("height")

                    num, den = video_stream.get("r_frame_rate").split("/")
                    fps = int(float(num) / float(den)) if den != "0" else 0

                    duration = float(data["format"]["duration"])

                    upload_url = upload_to_gcs_project(
                        projectID,
                        videoID,
                        thumbnail_name,
                        thumbnail_path
                    )

                    new_db.UpdateVideoThumbnailUrl(videoID, upload_url)

                    new_db.UpdateVideoChanges(
                        projectID,
                        videoID,
                        width,
                        height,
                        duration,
                        fps
                    )

                    new_db.InsertLogs(projectID, videoID, "thumbnail", "end", "end")

                except Exception as e:
                    logging.exception(e)
                    new_db.InsertLogs(projectID, videoID, "thumbnail", str(e), "exception") 

        # =====================================================
        # LIBRARY VIDEO
        # =====================================================
        else:

            videoUrls = new_db.GetLibraryVideo(videoID)

            for item in videoUrls:

                videoUrl = item.VideoUrl

                try:
                    thumbnail_name = f"LibraryVideoThumbnail_{item.ID}.jpg"
                    thumbnail_path = base_path / thumbnail_name

                    # -----------------------------
                    # PURE FFMPEG (REPLACES MOVIEPY)
                    # -----------------------------
                    cmd = [
                        "ffmpeg",
                        "-y",
                        "-ss", "1",
                        "-i", videoUrl,
                        "-frames:v", "1",
                        "-q:v", "2",
                        str(thumbnail_path)
                    ]

                    subprocess.run(cmd, check=True)

                    upload_url = upload_to_gcs_library(
                        item.ID,
                        thumbnail_name,
                        thumbnail_path
                    )

                    new_db.UpdateLibraryVideoThumbnailUrl(item.ID, upload_url)

                except Exception as e:
                    logging.exception(e)

        # -----------------------------
        # CLEANUP
        # -----------------------------
        if base_path.exists():
            shutil.rmtree(base_path)

        new_db.InsertLogs(videoID, 0, "thumbnail", "process_thumbnail", "end")

        return "success"

    except Exception as e:
        logging.exception("Fatal error in thumbnail job")

        new_db.InsertLogs(
            videoID if videoID else 0,
            0,
            "thumbnail",
            str(e),
            "fatal"
        )

        return "error"

def upload_to_gcs_project(projectID, videoID, file_name, local_path):

    bucket = storage_client.bucket(cfg.GCS_BUCKET_NAME)

    object_name = f"{cfg.GCS_DIRECTORY}/{projectID}/{file_name}"
    blob = bucket.blob(object_name)

    blob.upload_from_filename(str(local_path), content_type="image/jpeg")
    blob.make_public()

    return blob.public_url


# =====================================================
# GCS UPLOAD - LIBRARY
# =====================================================
def upload_to_gcs_library(videoID, file_name, local_path):

    bucket = storage_client.bucket(cfg.GCS_BUCKET_NAME)

    object_name = f"{cfg.GCS_DIRECTORY}/LibraryVideos/{file_name}"
    blob = bucket.blob(object_name)

    blob.upload_from_filename(str(local_path), content_type="image/jpeg")
    blob.make_public()

    return blob.public_url
