import os
import json
import requests
import logging
import subprocess

from handlers.thumbnails import process_thumbnail


def run_ffmpeg_thumbnail(input_path, output_path, time_sec=1):
    """
    Pure ffmpeg thumbnail extraction
    """
    cmd = [
        "ffmpeg",
        "-ss", str(time_sec),
        "-i", input_path,
        "-frames:v", "1",
        "-q:v", "2",
        output_path
    ]

    logging.info(f"Running ffmpeg: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(result.stderr)
        raise Exception("FFmpeg thumbnail generation failed")

    return output_path


def main():
     
    try:
        payload = os.environ.get("JOB_PAYLOAD")

        if not payload:
            raise Exception("JOB_PAYLOAD not provided")

        params = json.loads(payload)
        job_type = params.get("type")

        logging.info(f"Render Job Started: {job_type}")

        if job_type == "generate_thumbnail":
            process_thumbnail(params)

        else:
            raise Exception(f"Unknown job type: {job_type}")

        logging.info("Render Job Completed Successfully")

    except Exception as e:
        logging.exception("Render Job Failed")
        raise


if __name__ == "__main__":
    main()