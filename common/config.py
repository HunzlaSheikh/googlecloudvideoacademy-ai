import os


class Config:
    """
    Central configuration for API + Worker services.
    Works in Cloud Run using environment variables.
    """
    # -----------------------------
    # GOOGLE CLOUD CONFIG
    # -----------------------------
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

    # -----------------------------
    # VALIDATION (fail fast)
    # -----------------------------
    if not GCP_PROJECT:
        raise Exception("Missing environment variable: GCP_PROJECT")

    if not PUBSUB_TOPIC:
        raise Exception("Missing environment variable: PUBSUB_TOPIC")

    # -----------------------------
    # OPTIONAL SETTINGS
    # -----------------------------
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    ENVIRONMENT = os.environ.get("ENVIRONMENT", "staging")  # staging / production
    # =========================
    # DATABASE CONFIG (SQL Server)
    # =========================
    DBCONNSERV = os.environ.get('DBCONNSERV') or 'tcp:sqvdev.database.windows.net,1433;'
    DBASE = os.environ.get('DBASE') or 'videoacademyDev'
    DBBUUID = os.environ.get('DBBUUID') or 'sqvdbserr'
    DBPW = os.environ.get('DBPW') or 'TrudoDev2019!!'

    # =========================
    # GOOGLE CLOUD STORAGE
    # =========================
    GCS_BUCKET_NAME='sportsmarts-storeage'
    GCS_DIRECTORY='Staging'

    # =========================
    # API BASE URL (for emails, callbacks, etc.)
    # =========================
    WebAppUrl='https://sportsmartsapp-staging-dotnet8-4203741058.us-central1.run.app'
    ApiUrl='https://sportsmartsapi-staging-dotnet8-4203741058.europe-west1.run.app'

    # =========================
    # OPTIONAL: ENV TYPE
    # =========================
    ENV = os.getenv("ENV", "dev")  # dev / staging / prod

    # =========================
    # DEBUG MODE
    # =========================
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"