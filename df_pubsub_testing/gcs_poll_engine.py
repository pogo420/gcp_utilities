from google.cloud import pubsub_v1
from json import load
import time


def poll_notifications(project, subscription_name, log_file):
    """Polls a Cloud Pub/Sub subscription for new GCS events for display."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        # with open(log_file, "a") as f:
        #     temp = dict([(i, message.attributes[i]) for i in message.attributes.keys()])
        #     f.write(str(temp))
        #     f.write("\r\n")
        #     message.ack()
        # print(message)
        message_dict = message.attributes
        if message_dict.get("eventType") == "OBJECT_FINALIZE":
            response = "{0}:{1}/{2}".format(
                message_dict.get(
                    "eventTime"), message_dict.get(
                        "bucketId"), message_dict.get("objectId"))
            print(response)
            message.ack()
        else:
            message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)


def main():
    PROJECT_ID = ""
    PS_SUBSCRIPTION_NAME = ""
    PS_LOG_FILE = ""
    print("reading config file..")
    with open("config.json") as f:
        config_data = load(f)
        PROJECT_ID = config_data.get("PROJECT_ID")
        PS_SUBSCRIPTION_NAME = config_data.get("PS_SUBSCRIPTION_NAME")
        PS_LOG_FILE = config_data.get("PS_LOG_FILE")

    poll_notifications(
        project=PROJECT_ID, subscription_name=PS_SUBSCRIPTION_NAME,
        log_file=PS_LOG_FILE)


if __name__ == "__main__":
    main()
