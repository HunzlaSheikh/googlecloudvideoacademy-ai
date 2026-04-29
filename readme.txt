/api
  app.py
  pubsub_client.py
  Dockerfile
 

/worker
  - common
     - __init__.py
     - config.py
     - dbintegration.py
  - handlers
     - aieventdetect.py
     - thumbnails.py
  app.py
  Dockerfile
  requirements.txt


  -------------------------------
  
gcloud run deploy videoai-worker-staging --source . --region us-central1