import json
import base64
import requests
import google.auth
from google.auth.transport.requests import Request


PROJECT_ID = "gen-lang-client-0215307817"
LOCATION = "us-central1"
COMPOSER_ENV = "composer-medicare"
DAG_ID = "main_pipeline_orchestrator"


def trigger_airflow(event, context):
    """
    Trigger Composer DAG from Pub/Sub event.
    """

    try:
        if "data" not in event:
            print("No Pub/Sub message found")
            return

        # Decode Pub/Sub message
        message_data = base64.b64decode(
            event["data"]
        ).decode("utf-8")

        payload = json.loads(message_data)

        bucket = payload.get("bucket")
        file_name = payload.get("name")

        print(
            f"New file detected: "
            f"gs://{bucket}/{file_name}"
        )

        # Ignore non-raw uploads
        if not file_name.startswith("raw/"):
            print("Ignoring non-raw file")
            return

        # Get Composer environment details
        composer_url = (
            f"https://composer.googleapis.com/v1/"
            f"projects/{PROJECT_ID}/locations/"
            f"{LOCATION}/environments/"
            f"{COMPOSER_ENV}"
        )

        credentials, _ = google.auth.default()
        credentials.refresh(Request())

        env_response = requests.get(
            composer_url,
            headers={
                "Authorization":
                f"Bearer {credentials.token}"
            }
        )

        env_json = env_response.json()
        print(env_json)

        airflow_uri = (
            env_json["config"]["airflowUri"]
        )

        trigger_url = (
            f"{airflow_uri}/api/v1/dags/"
            f"{DAG_ID}/dagRuns"
        )

        dag_payload = {
            "conf": {
                "triggered_by": "pubsub",
                "file_name": file_name
            }
        }

        response = requests.post(
            trigger_url,
            headers={
                "Authorization":
                f"Bearer {credentials.token}",
                "Content-Type":
                "application/json"
            },
            json=dag_payload
        )

        print(
            f"Airflow response: "
            f"{response.status_code}"
        )
        print(response.text)

    except Exception as e:
        print(f"Error: {str(e)}")
        raise