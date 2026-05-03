import os
import json
import time
import logging
import pathlib
import shutil
import subprocess
import numpy as np
import pandas as pd
import pyodbc
import boto3
import requests

from datetime import date, datetime, timedelta
from scipy.io import wavfile
from pydub import AudioSegment

from common import dbintegeration as db
from common.config import Config

cfg = Config()
 
rekognition_client = boto3.client(
    'rekognition',
    region_name='xxxxxx',
    aws_access_key_id='xxxxxxxxxx',
    aws_secret_access_key='xxxxxxxxxxxx'
)

projectArn = db.GetSysteParamValue('AWSProjectArn')
projectVersionArn = db.GetSysteParamValue('AWSProjectVersionArn')

 
def process_ai_event_detect(params):

    video_id = params.get("videoId")
    project_id = params.get("projectId")
    domain_id = params.get("domainId")

    base_path = pathlib.Path("/tmp") / str(video_id)
    frames_path = base_path / "frames"
    audio_path = base_path / "audio.wav"

    try: 
        frames_path.mkdir(parents=True, exist_ok=True)
 
        video_url = db.GetVideoUrlById(video_id)
        db.InsertLogs(project_id,video_id,"vaeventdetectnuig","extract frames ffmpeg subprocess","start")
        subprocess.run([
            "ffmpeg",
            "-i", video_url,
            "-vf", "fps=1",
            str(frames_path / "frame_%04d.jpg")
        ], check=True, timeout=600)
        db.InsertLogs(project_id,video_id,"vaeventdetectnuig","extract frames ffmpeg subprocess","end")
        
        db.InsertLogs(project_id,video_id,"vaeventdetectnuig","extract audio ffmpeg subprocess","start")
       
        subprocess.run([
            "ffmpeg",
            "-i", video_url,
            "-vn",
            "-acodec", "pcm_s16le",
            "-ar", "16000",
            "-ac", "1",
            str(audio_path)
        ], check=True, timeout=600)

        db.InsertLogs(project_id,video_id,"vaeventdetectnuig","extract audio ffmpeg subprocess","end")
        
        predictions = run_boto3_inference(
            project_id, video_id, list(frames_path.glob("*.jpg"))
        )

        audio_duration = get_audio_duration(audio_path)
        whistle_times = find_whistles(
            project_id, video_id, audio_path, {}, 0, audio_duration
        )
        
        config = {
            "output": {
                "events_path": str(base_path / "events.csv"),
                "customvision_print_path": str(base_path / "predictions.csv")
            },
            "event_detection": {
                "fps": 1,
                "confidence_threshold": 0.5
            }
        }
        extend_events_list(project_id,video_id, config)
        db.InsertLogs(project_id,video_id,"vaeventdetectnuig","getting custom vision predictions from db","info")
        custom_predictions = PrintCustomVisionPredictions(
            video_id, project_id, config["output"]["customvision_print_path"]
        )

        inserted_events = db.load_data_into_db_new_logic(
            project_id, video_id, domain_id, config, custom_predictions
        ) 
        return "success"

    except Exception as e:
        logging.exception("AI pipeline failed")
        return "error"

    finally:
        if base_path.exists():
            shutil.rmtree(base_path, ignore_errors=True)

 
def start_rekognition_model_if_required(project_id, video_id):

    response = rekognition_client.describe_project_versions(
        ProjectArn=projectArn
    )

    status = response['ProjectVersionDescriptions'][0]['Status']

    db.InsertLogs(project_id, video_id, "model", f"status: {status}", "info")

    if status != "RUNNING":
        rekognition_client.start_project_version(
            ProjectVersionArn=projectVersionArn,
            MinInferenceUnits=1
        )

    while True:
        response = rekognition_client.describe_project_versions(
            ProjectArn=projectArn
        )
        status = response['ProjectVersionDescriptions'][0]['Status']

        if status == "RUNNING":
            break

        time.sleep(5)


def stop_rekognition_if_idle(project_id, video_id):
    db.InsertLogs(project_id, video_id,"vaeventdetectnuig","stopping aws rekognize model","start")  
    
    ai_processing = db.GetAIProcessingVideos()
    db.InsertLogs(project_id, video_id,"vaeventdetectnuig",f"ai_processing videos: {len(ai_processing)}","info")  
    
    if len(ai_processing) == 0:
        rekognition_client.stop_project_version(
            ProjectVersionArn=projectVersionArn
        )
    db.InsertLogs(project_id, video_id,"vaeventdetectnuig","stopping aws rekognize model","end")  
    
    
 
def run_boto3_inference(project_id, video_id, frame_paths):
    db.InsertLogs(project_id, video_id,"vaeventdetectnuig","run_boto3_inference","start")  
    results = []
    db.UpdateProjectVideoAIProcessing(video_id, 1)
    start_rekognition_model_if_required(project_id, video_id)
    for frame_path in sorted(frame_paths):
        db.InsertLogs(project_id, video_id,"vaeventdetectnuig",f"predicting image path: {frame_path}","info")  
                    
        with open(frame_path, "rb") as img:
            response = rekognition_client.detect_custom_labels(
                Image={'Bytes': img.read()},
                ProjectVersionArn=projectVersionArn
            )

        labels = response.get("CustomLabels", [])

        if labels:
            top = labels[0]
            results.append({
                "frame": frame_path.name,
                "prediction": top["Name"],
                "confidence": top["Confidence"] / 100
            })
        else:
            results.append({
                "frame": frame_path.name,
                "prediction": "Negative",
                "confidence": 0
            })
    db.UpdateProjectVideoAIProcessing(video_id, 0)
    stop_rekognition_if_idle(project_id, video_id)
    db.InsertLogs(project_id, video_id,"vaeventdetectnuig","run_boto3_inference","end")  
    return results

 
def get_audio_duration(audio_path):
    audio = AudioSegment.from_wav(audio_path)
    return len(audio) / 1000

 
def find_whistles(projectId, videoId, audio_path, config, chunk, audio_duration):

    logging.info("find whistles audio path %s", audio_path)

    db.InsertLogs(projectId, videoId,"vaeventdetectnuig",f"find whistles audio path: {audio_path}","info")
    pairs = []
    temp_files = []

    db.InsertLogs(projectId, videoId,"vaeventdetectnuig",f"find whistles detection","start")
    try:

        if audio_duration > 900:
            orgAudio = AudioSegment.from_wav(str(audio_path))
            eachChunkFrame = int(audio_duration / 900)

            for i in range(eachChunkFrame + 1):

                t1 = (900 * i) * 1000
                t2 = min((900 * i + 900), audio_duration) * 1000

                chunk_audio = orgAudio[t1:t2]

                tmp_file = f"/tmp/tmp_audio_{projectId}_{i}.wav"
                chunk_audio.export(tmp_file, format="wav")

                temp_files.append(tmp_file)

                samplerate, wavdata = wavfile.read(tmp_file)

                if wavdata.size > 0:
                    energy = np.sum(np.abs(wavdata))

                    if energy > 5e7:
                        pairs.append((i, "whistle_detected"))

        else:
            samplerate, wavdata = wavfile.read(audio_path)

            energy = np.sum(np.abs(wavdata))

            if energy > 5e7:
                pairs.append((0, "whistle_detected"))

    except Exception as e:
        logging.exception("whistle detection error")
        return []

    finally:
        for f in temp_files:
            if os.path.exists(f):
                os.remove(f)

    db.InsertLogs(projectId, videoId,"vaeventdetectnuig",f"find whistles detection","end")
    return pairs

 
def extend_events_list(projectId, videoId, config):

    if not os.path.exists(config['output']['events_path']):
        return []

    events_csv = pd.read_csv(config['output']['events_path'])

    events_grp = []
    whistles = []

    for i in range(len(events_csv)):

        if events_csv['class'][i] != 'GenFootage':

            st = events_csv['starttime'][i]
            et = events_csv['endtime'][i]

            ftr = [3600, 60, 1]

            st_sec = np.ceil(sum([a * b for a, b in zip(ftr, map(float, st.split(':')))]))
            et_sec = np.ceil(sum([a * b for a, b in zip(ftr, map(float, et.split(':')))]))

            events_grp.append((events_csv['class'][i], st_sec, et_sec))

        elif events_csv['class'][i] == 'whistle':
            whistles.append(('whistle', events_csv['starttime'][i], events_csv['endtime'][i]))

    return events_grp + whistles

 
def PrintCustomVisionPredictions(videoId, projectId, predictionpath):

    predictionAndConfidence = db.GetProjectVideoPredictionsConfidences(videoId, projectId)

    custom_predictions = []

    for item in predictionAndConfidence:

        custom_predictions.append({
            "frame": item.Frame,
            "prediction": item.Predictions,
            "confidence": item.Confidence,
            "tag_name": item.TagName
        })

    return custom_predictions


 