/api
  app.py
  pubsub_client.py
  Dockerfile
 

/worker
  /common
    - __init__.py
    - config.py
    - dbintegration.py 
  /render
    /handlers 
     - thumbnails.py
    main.py
    Dockerfile
    requirements.txt 
  /ai  
    /handlers
     - aieventdetect.py 
    main.py
    Dockerfile
    requirements.txt


  -------------------------------
  
gcloud run deploy videoai-api-staging --source . --region us-central1
gcloud run jobs deploy video-render-job-staging --source ./worker/render --region us-central1 --tasks 1 --memory 2Gi --cpu 1 --max-retries 3 --set-env-vars PYTHONPATH=/app