import os
import logging
import pathlib
import shutil

from moviepy.editor import VideoFileClip
from google.cloud import storage

# =========================
# UPDATED IMPORTS (IMPORTANT)
# =========================
from common import dbintegeration as new_db
from common.config import Config


cfg = Config()
storage_client = storage.Client()


def process_thumbnail(params):
    """
    Cloud Run worker handler for thumbnail generation
    """

    projectID = 0

    try:
        videoID = params.get("videoID")
        videoType = params.get("videoType")

        logging.info(f"Thumbnail started for videoID={videoID}")

        new_db.InsertLogs(
            0, videoID,
            "thumbnail",
            "process_thumbnail",
            "start"
        )

        # -----------------------------
        # Temp folder
        # -----------------------------
        base_path = pathlib.Path(__file__).parent.parent
        file_path = base_path / "temp" / str(videoID)
        file_path.mkdir(parents=True, exist_ok=True)

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

                    thumbnail_name = f"VideoThumbnail_{videoID}_{projectID}.jpeg"
                    thumbnail_path = file_path / thumbnail_name

                    with VideoFileClip(videoUrl) as clip:
                        clip.save_frame(str(thumbnail_path), t=1.0)

                        width = clip.w
                        height = clip.h
                        duration = clip.duration
                        fps = int(clip.fps)

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

                    new_db.InsertLogs(
                        projectID,
                        videoID,
                        "thumbnail",
                        str(e),
                        "exception"
                    )

                    send_email(projectID, videoID, str(e))

        # =====================================================
        # LIBRARY VIDEO
        # =====================================================
        else:

            videoUrls = new_db.GetLibraryVideo(videoID)

            for item in videoUrls:

                videoUrl = item.VideoUrl

                try:
                    thumbnail_name = f"LibraryVideoThumbnail_{item.ID}.jpeg"
                    thumbnail_path = file_path / thumbnail_name

                    with VideoFileClip(videoUrl) as clip:
                        clip.save_frame(str(thumbnail_path), t=1.0)

                    upload_url = upload_to_gcs_library(
                        item.ID,
                        thumbnail_name,
                        thumbnail_path
                    )

                    new_db.UpdateLibraryVideoThumbnailUrl(item.ID, upload_url)

                except Exception as e:
                    logging.exception(e)

        # -----------------------------
        # Cleanup
        # -----------------------------
        if file_path.exists():
            shutil.rmtree(file_path)

        new_db.InsertLogs(videoID, 0, "thumbnail", "process_thumbnail", "end")

        return "success"


    except Exception as e:
        logging.exception("Fatal error in thumbnail worker")

        new_db.InsertLogs(
            videoID if 'videoID' in locals() else 0,
            0,
            "thumbnail",
            str(e),
            "fatal"
        )

        return "error"


# =====================================================
# GCS UPLOAD - PROJECT
# =====================================================
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


# =====================================================
# EMAIL (unchanged placeholder)
# =====================================================
def send_email(projectID, videoID, exception):
    try:
        new_db.InsertLogs(projectID, videoID, "thumbnail", "send_email", "start")
    except Exception as e:
        logging.exception(e)