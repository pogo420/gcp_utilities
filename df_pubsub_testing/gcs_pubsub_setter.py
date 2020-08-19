import subprocess
from json import load

WATCH_BUCKET = ""
PS_TOPIC_NAME = ""
PROJECT_ID = ""
PS_SUBSCRIPTION_NAME = ""
print("reading config file..")
with open("config.json") as f:
    config_data = load(f)
    WATCH_BUCKET = config_data.get("WATCH_BUCKET")
    PS_TOPIC_NAME = config_data.get("PS_TOPIC_NAME")
    PROJECT_ID = config_data.get("PROJECT_ID")
    PS_SUBSCRIPTION_NAME = config_data.get("PS_SUBSCRIPTION_NAME")

print("Initializing project..")
subprocess.run([
    "gcloud", "config", "set", "project", PROJECT_ID])

print("Creating pub sub topic and linking wih GCS..")
subprocess.run([
    "gsutil", "notification", "create", "-t", PS_TOPIC_NAME, "-f", "json",
    WATCH_BUCKET])

print("Creating pub sub subscription..")
subprocess.run([
    "gcloud", "beta", "pubsub", "subscriptions", "create",
    PS_SUBSCRIPTION_NAME, "--topic={0}".format(PS_TOPIC_NAME)])
