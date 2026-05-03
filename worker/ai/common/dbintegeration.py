import pyodbc
import logging
from datetime import datetime 
import os
from datetime import date
from common import config
import pandas as pd
cfg = config.Config()

 
def get_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={cfg.DBCONNSERV};'
        f'DATABASE={cfg.DBASE};'
        f'UID={cfg.DBBUUID};'
        f'PWD={cfg.DBPW}'
    )

 
def GetSysteParamValue(value):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        query = f"SELECT ParamValue FROM SysParams WHERE ParamName='{value}'"
        cursor.execute(query)

        row = cursor.fetchone()

        cnxn.close()

        return row[0] if row else None

    except Exception as e:
        logging.exception(e)
        return None


# =====================================================
# LOGGING
# =====================================================
def InsertLogs(ProjectID, VideoID, ServiceName, Description, Status):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        query = """
        INSERT INTO tbl_Logs (ProjectID, VideoID, ServiceName, Description, Status)
        VALUES (?, ?, ?, ?, ?)
        """

        cursor.execute(query, ProjectID, VideoID, ServiceName, Description, Status)

        cnxn.commit()
        cnxn.close()

    except Exception as e:
        logging.exception(e)


# =====================================================
# PROJECT VIDEO
# =====================================================
def GetProjectVideoUrls(videoID):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        sql = """
        SELECT Url, VoiceOverUrl, ProjectId, ClipsRenderedUrl
        FROM ProjectVideos
        WHERE ID = ?
        """

        cursor.execute(sql, videoID)
        results = cursor.fetchall()

        cnxn.close()
        return results

    except Exception as e:
        logging.exception(e)
        return []

def GetVideoUrlById(videoId):
    query =''' select Url from ProjectVideos where Id='{}' '''.format(videoId)
    cnxn = get_connection()
    cursor = cnxn.cursor()                    
    cursor.execute(query)
    
    for row in cursor:
        videoUrl = row[0]
        
          
    return videoUrl

def GetProjectVideoAnnotation(videoID):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        sql = """
        SELECT Type,StartTime,EndTime,Text,Fill,FontFamily,FontSize,
               Height,[Left],Opacity,TextBackgroundColor,[Top],Width,
               Url,Angle,CanvasHeight,CanvasWidth,RenderTop,RenderLeft
        FROM ProjectVideoAnnotations
        WHERE VideoID = ?
        """

        cursor.execute(sql, videoID)
        results = cursor.fetchall()

        cnxn.close()
        return results

    except Exception as e:
        logging.exception(e)
        return []


# =====================================================
# UPDATE VIDEO
# =====================================================
def UpdateVideoThumbnailUrl(videoID, thumbnailUrl):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        cursor.execute(
            "UPDATE ProjectVideos SET ThumbnailUrl=? WHERE ID=?",
            thumbnailUrl, videoID
        )

        cnxn.commit()
        cnxn.close()

    except Exception as e:
        logging.exception(e)


def UpdateVideoChanges(projectID, videoID, width, height, duration, fps):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        sql = """
        UPDATE ProjectVideos
        SET Width=?, Height=?, Duration=?, Fps=?
        WHERE ID=?
        """

        cursor.execute(sql, width, height, duration, fps, videoID)

        cnxn.commit()
        cnxn.close()

        InsertLogs(
            projectID,
            videoID,
            "video_update",
            "Video metadata updated",
            "info"
        )

    except Exception as e:
        logging.exception(e)


# =====================================================
# LIBRARY VIDEO
# =====================================================
def GetLibraryVideo(videoID):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        cursor.execute("""
        SELECT Name, VideoUrl, ID
        FROM LibraryVideo
        WHERE ID = ?
        """, videoID)

        results = cursor.fetchall()
        cnxn.close()

        return results

    except Exception as e:
        logging.exception(e)
        return []


def UpdateLibraryVideoThumbnailUrl(videoID, thumbnailUrl):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        cursor.execute("""
        UPDATE LibraryVideo
        SET ThumbnailUrl=?
        WHERE ID=?
        """, thumbnailUrl, videoID)

        cnxn.commit()
        cnxn.close()

    except Exception as e:
        logging.exception(e)


# =====================================================
# ERROR LOG
# =====================================================
def ErrorLog(serviceName, error):
    try:
        cnxn = get_connection()
        cursor = cnxn.cursor()

        cursor.execute("""
        INSERT INTO ErrorLog (Date, ServiceName, Error)
        VALUES (?, ?, ?)
        """, datetime.now(), serviceName, str(error))

        cnxn.commit()
        cnxn.close()

    except Exception as e:
        logging.exception(e)

def GetProjectVideoPredictionsConfidences(videoId,projectId):        
        cnxn = get_connection()  
        query =''' select  Confidence,Predictions,Frame,TagName from tbl_ImageClassificationProcessing where ProjectId='{0}' and   VideoId='{1}' order by Frame '''.format(projectId,videoId)                    
        updatecursor = cnxn.cursor()
        updatecursor.execute(query)
        results = updatecursor.fetchall()
        cnxn.close()
        del cnxn    

        return results

def get_ids_from_db(eventname,domainId):
    query = 'SELECT DISTINCT ID, ProjectID, Url from [dbo].[ProjectVideos];'
    cnxn = get_connection()  
    cursor = cnxn.cursor()                    
    cursor.execute(query) 
    
    query = "SELECT Id FROM [dbo].[Events] WHERE EventName='{0}' and DomainId='{1}';".format(eventname,domainId)  
    
    cursor.execute(query)
    for row in cursor:
        EventID = row[0]
    cnxn.close()
    del cnxn
    return  EventID
def load_data_into_db_new_logic(projectId, videoId, domainId, config, custom_predictions):

    InsertLogs(projectId, videoId, "vaeventdetectnuig", "load_data_into_db start", "start")

    ftr = [3600, 60, 1]
    fps = config['event_detection']['fps']
    threshold = config['event_detection']['confidence_threshold']

    customevents = custom_predictions or []
    events = []
    insertedEvents = []

    cnxn = get_connection()

    cursor = cnxn.cursor()
 
    current_event = None
    diff = 0
    start = 1

    for i, ev in enumerate(customevents):

        if ev['confidence'] >= threshold and ev['tag_name'] != 'Negative':

            if current_event is None:
                current_event = [ev['tag_name'], start / fps, start / fps]

            elif current_event[0] != ev['tag_name']:

                if diff <= 3 * fps:
                    events.append(current_event)

                current_event = [ev['tag_name'], start / fps, start / fps]
                diff = 0

            else:
                current_event[2] = start / fps
                diff = 0

        else:
            diff += 1
            if diff > 3 * fps and current_event:
                events.append(current_event)
                current_event = None
                diff = 0

        start += 1

    if current_event:
        events.append(current_event)

    InsertLogs(projectId, videoId, "vaeventdetectnuig", f"events grouped: {len(events)}", "info")
 
    for ev in events:

        try:
            start_sec = ev[1]
            end_sec = ev[2]
            eventname = ev[0]

            overlap = any(
                a['start_time'] <= start_sec <= a['end_time']
                for a in insertedEvents
            )

            if overlap:
                continue

            today = str(date.today())

            eventId = get_ids_from_db(  eventname, domainId)

            query = f"""
            INSERT INTO ProjectEvents
            (EventId, ProjectId, ProjectVideoID, EventOccurTime, EventType, CreatedOn, EventEndTime, SystemGeneratedEvent)
            VALUES
            ({eventId}, {projectId}, {videoId}, {start_sec}, 'For', '{today}', {end_sec}, 'True')
            """

            cursor.execute(query)
            insertedEvents.append({
                "start_time": start_sec,
                "end_time": end_sec,
                "eventname": eventname
            })

        except Exception as e:
            InsertLogs(projectId, videoId, "vaeventdetectnuig", str(e), "db error")

    cnxn.commit()
 
    if os.path.exists(config['output']['events_path']):

        df = pd.read_csv(config['output']['events_path'])

        for _, row in df.iterrows():

            if row['class'] != 'whistle':
                continue

            start_sec = sum(x * int(t) for x, t in zip(reversed(ftr), reversed(row['starttime'].split(":")))) - 1
            end_sec = sum(x * int(t) for x, t in zip(reversed(ftr), reversed(row['endtime'].split(":"))))

            eventname = "Referee Whistle"

            eventId = get_ids_from_db(  eventname, domainId)

            query = f"""
            INSERT INTO ProjectEvents
            (EventId, ProjectId, ProjectVideoID, EventOccurTime, EventType, CreatedOn, EventEndTime, SystemGeneratedEvent)
            VALUES
            ({eventId}, {projectId}, {videoId}, {start_sec}, 'For', '{str(date.today())}', {end_sec}, 'True')
            """

            cursor.execute(query)

    cnxn.commit()
    cnxn.close()

    InsertLogs(projectId, videoId, "vaeventdetectnuig", "load_data_into_db end", "end")

    return insertedEvents


def UpdateProjectVideoAIProcessing(videoId, IsAIProcessing):
    try:
        query = '''Update ProjectVideos set IsAIProcessing =? where ID= ? '''
        with pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg.DBCONNSERV};DATABASE={cfg.DBASE};UID={cfg.DBBUUID};PWD={cfg.DBPW}'
            ) as cnxn:
            logcursor = cnxn.cursor()
            logcursor.execute(query, IsAIProcessing, videoId)
            cnxn.commit()
    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error in DeleteClassificationImageProcessing: {db_err}")
    except Exception as e:
        logging.error(f"General error in DeleteClassificationImageProcessing: {e}")