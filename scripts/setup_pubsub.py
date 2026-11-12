"""Create the Pub/Sub topic and subscription (works on the emulator and on real GCP)."""
import os
import sys

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

project = os.getenv("GCP_PROJECT", "local-fraud-demo")
topic = os.getenv("PUBSUB_TOPIC", "transactions")
sub = os.getenv("PUBSUB_SUBSCRIPTION", "transactions-sub")

pub, subscriber = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
topic_path, sub_path = pub.topic_path(project, topic), subscriber.subscription_path(project, sub)

# --reset: drop the subscription first so a demo starts with an empty backlog
if "--reset" in sys.argv:
    try:
        subscriber.delete_subscription(request={"subscription": sub_path})
        print("deleted", sub_path)
    except Exception:
        pass
for create, args in [(pub.create_topic, {"name": topic_path}),
                     (subscriber.create_subscription, {"name": sub_path, "topic": topic_path, "ack_deadline_seconds": 60})]:
    try:
        create(request=args)
        print("created", args["name"])
    except AlreadyExists:
        print("exists ", args["name"])
