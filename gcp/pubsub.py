"""
Python package to interact with Google Pub/Sub. 

Requirements: · Enable Pub/Sub API in GCP
              · Pub/Sub Publisher role enabled on Service Account
              · Pub/Sub Subscriber role enabled on Service Account
              · Create a topic in GCP (You can also locally create topics with the gcloud commands)
              · Create a subscription in GCP (Can also be done locally with gcloud)

----------------------------------------------------------------------------------------------------------
--------------------------------------------Using the Emulator--------------------------------------------
----------------------------------------------------------------------------------------------------------
Steps for creating a Pub/Sub Emulator testing environment on a Windows PC:
    NOTE: It is useful to create a virtual environment, but not necessary. 
    Prior to installing the emulator, confirm these are installed:
        · Latest version of Python
        · Java JRE version 7 or higher
        · Google Cloud CLI
          
  A) Within the Google CLI, install the emulator by running:
    >>> gcloud components install pubsub-emulator
    >>> gcloud components update

  B) Using cmd prompt, start the emulator by running:
    >>> gcloud beta emulators pubsub start --project=PUBSUB_PROJECT_ID 
    NOTE: PUBSUB_PROJECT_ID does not need to represent a real GCP project, because the emulator runs locally.
          The emulator listen on port 8085 on your IPv6 localhost by default. 

  C) Set environment variables (Automatically) - enter the following in cmd prompt:
    >>> gcloud beta emulators pubsub env-init > set_vars.cmd && set_vars.cmd
     
  D) Using the emulator:
        1) Clone the Pub/Sub Python repo here: https://github.com/googleapis/python-pubsub
        2) Navigate to samples/snippets directory, complete the following steps within that directory. 
        3) Install dependencies needed:
            >>> pip install -r requirements.txt
        4) Create a topic:
            >>> python publisher.py PUBSUB_PROJECT_ID create TOPIC_ID
        5) Create a local push endpoint:
            · Create on http://[::1]:3000/messages (This will be referred to later on as PUSH_ENDPOINT)
            a) Install JSON Server:
                >>> npm install -g json-server
            b) Start JSON Server:
                >>> json-server --port 3000 --watch db.json
                · db.json contains this starter code:
                    {
                        "messages": []
                    }
        6) Create a subscription to the topic:
            · Pull sub:
                >>> python subscriber.py PUBSUB_PROJECT_ID create TOPIC_ID SUBSCRIPTION_ID
            · Push sub:
                >>> python subscriber.py PUBSUB_PROJECT_ID create-push TOPIC_ID SUBSCRIPTION_ID \
                    PUSH_ENDPOINT
        7) Publish messages to topic:
            >>> python publisher.py PUBSUB_PROJECT_ID publish TOPIC_ID
        8) Read published messages:
            · Retrieve messages from pull subscription:
                >>> python subscriber.py PUBSUB_PROJECT_ID receive SUBSCRIPTION_ID
            · Confirm the messages were delivered to the local push endpoint, 
              messages should look like the following:
                {
                    "messages": [
                        {
                            "subscription": "projects/PUBSUB_PROJECT_ID/subscriptions/SUBSCRIPTION_ID",
                            "message": {
                                "data": "TWVzc2FnZSBudW1iZXIgMQ==",
                                "messageId": "10",
                                "attributes": {}
                            },
                            "id": 1
                        },
                        ...
                    ]
                }
"""

import base64
import json
from concurrent import futures
from logging import Logger
from typing import Any, Callable

import google.auth
from requests import Response
from urllib import request
from concurrent import futures
from logging import Logger
from typing import Any, Callable
from flask import current_app, json
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import DeadLetterPolicy
from concurrent import futures
from typing import Callable, Optional

from flask import current_app, json
from google.cloud import pubsub_v1


class PubSubClient(object):
    def __init__(
        self, logger: Logger, topic_name: str, project_id: str, push_endpoint: str
    ):
        self._project_id = project_id
        self._topic_name = topic_name
        self._credentials, _ = google.auth.default()
        self._subscriber = pubsub_v1.SubscriberClient()
        self._publisher = pubsub_v1.PublisherClient()
        self._pubsub_prefix = current_app.config["PUBSUB_PREFIX"]
        self._topic_path = self._publisher.topic_path(
            self._project_id, self._topic_name
        )
        self._push_endpoint = (
            push_endpoint  # TODO: is this the right way to set this up?
        )
        self._logger = logger

    # Is there a decorator I can use here to avoid having to re-define within the pub?
    def get_callback(
        self, future: pubsub_v1.publisher.futures.Future, data: str
    ) -> None:
        """Unwrapped callback"""
        try:
            self._logger.info("Published message %s.", future.result(timeout=60))
        except futures.TimeoutError as e:
            self._logger.error("Publishing {data} timed out. {e}")

    # Publisher
    def pub(self, payload: json) -> None:
        """
        Publishes a payload to a topic.

        Parameters
        ----------
        payload : json
            JSON object to publish.

        Returns
        -------
        None
        """

        def get_callback_pub(future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                self._logger.info(f"Published message %s.", future.result(timeout=60))
            except futures.TimeoutError as e:
                self._logger.error("Publishing {data} timed out. {e}")

        try:
            # When a message is published, Client returns a "future".
            data = json.dumps(payload).encode("utf-8")
            future = self._publisher.publish(self._topic_path, data=data)
            get_callback_pub(self, future, data)
            future.add_done_callback(get_callback_pub(future, data))
            future.append(future)
            futures.wait(future, return_when=futures.ALL_COMPLETED)
            self._logger.info(f"Pushed message to topic: {self._topic_path}")

        except Exception as e:
            self._logger.error(
                f"Error! There was a problem when attempting to publish: {e}"
            )
        self._logger.info("Complete.")

    # Subscriber and accompanying functions
    def sub(
        self,
        subscription: str,
        timeout: int,
        dead_letter_topic: pubsub_v1.types.Topic,
        flow_control_max_messages: int = 50,
        max_delivery_attempts: int = 25,
        message_processing: Callable = lambda x: None,
        push_or_pull: str = "pull",
    ) -> None:
        """
        Consumer function to consume message from the topic given when initializing the Client.

        Parameters
        ----------

        subscription : str
            Subscription name.
        timeout : int
            Timeout for subscriber in seconds.
        dead_letter_topic : pubsub_v1.types.Topic
            An existing topic path for failed messages to be delivered to.
        flow_control_max_messages : int
            Hard limit for the subscriber to have a maximum of outstanding messages.
        max_delivery_attempts : int
            The number of attempts made before a message is delivered to the dead_letter_topic.
        push_or_pull : str
            Is this a push or pull type subscription?

        Returns
        -------
        200 when successful, 400 otherwise.
        """

        def get_callback_sub(message: pubsub_v1.subscriber.message.Message) -> None:
            self._logger.info(
                f"Received message {message} with message ID: {message.message_id}"
            )
            self._logger.info(message.data)
            try:
                message_processing(message.data)
                message.ack()
                self._logger.info(
                    f"Acknowledged message with message ID: {message.message_id}"
                )
                self._logger.info("Callback Complete.")
            except Exception as e:
                self._logger.error(
                    f"Error! There was a problem while attempting to consume messages: {e}"
                )

        # setup dead letter policy
        dlp = self._subscriber.types.DeadLetterPolicy(
            dead_letter_topic, max_delivery_attempts
        )
        # control message flow
        flow_control = pubsub_v1.types.FlowControl(
            max_messages=flow_control_max_messages
        )
        # configure retry backoff
        my_retry = pubsub_v1.types.RetryPolicy(
            minimum_backoff=pubsub_v1.types.Duration(seconds=10),
            maximum_backoff=pubsub_v1.types.Duration(seconds=600),
        )
        try:
            # if this is a push subscription
            if push_or_pull == "push":
                # configure push subscription
                push_config = pubsub_v1.types.PushConfig(
                    push_endpoint=self._push_endpoint  # TODO: decide how to make this dynamic?
                )
                subscription_path = self._subscriber.subscription_path(
                    self._project_id, subscription
                )
                subscription = self._subscriber.subscribe(
                    name=subscription_path,
                    topic=self._topic_path,
                    push_config=push_config,
                    retry_policy=my_retry,
                    ack_deadline_seconds=60,
                    dead_letter_policy=dlp,
                )
                with self._subscriber:
                    result = self._subscriber.update_subscription(
                        request={"subscription": subscription}
                    )
                    envelope = result.get_json()
                    if not envelope:
                        msg = "no Pub/Sub message recieved"
                        self._logger.error(f"Error! {msg}")
                        return f"Bad request: {msg}", 400
                    if not isinstance(envelope, dict) or "message" not in envelope:
                        msg = "invalid Pub/Sub message format"
                        self._logger.error(f"Error! {msg}")
                        return f"Bad Request: {msg}", 400
                    self._logger.debug("Pub/Sub push confirmed")
                    pubsub_message = envelope["message"]
                    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
                        json_object = json.loads(
                            base64.b64decode(pubsub_message["data"]).decode("utf-8")
                        )
                    else:
                        self._logger.error("No data in message")
                        return Response(status=400, response="No data in message")

            # if this is a pull subscription
            elif push_or_pull == "pull":
                subscription_path = self._subscriber.subscription_path(
                    self._project_id, subscription
                )
                streaming_pull_future = self._subscriber.subscribe(
                    subscription_path,
                    callback=get_callback_sub,
                    flow_control=flow_control,
                    timeout=timeout,
                )
                self._logger.info(f"Listening for messages on {subscription_path}...")
                with self._subscriber:
                    request = {
                        "name": subscription_path,
                        "topic": self._topic_path,
                        "future_streaming": streaming_pull_future,
                        "dead_letter_policy": dlp,
                    }
                subscription = self._subscriber.create_subscription(request)
                self._logger.info(f"Subscription created: {subscription.name}")
                self._logger.info(
                    f"It will forward dead letter messages to: {dead_letter_topic}."
                )
                self._logger.info(f"After {max_delivery_attempts} delivery attempts.")
                # replacing below code with subscriber using DeadLetter above
                # try:
                #     future_streaming.result(timeout=timeout)
                # except TimeoutError:
                #     future_streaming.cancel()
                return Response(msg="Complete", status=200)
            else:
                return Response(
                    msg="Invalid option passed to Push or Pull parameter.", status=400
                )

        except Exception as e:
            self._logger.error(f"Error! While attempting subscription: {e}")
            return Response(msg=e, status=400)

    def create_topic(self, topic_name: str) -> None:
        """
        Creates a new topic of the given name, uses topic_path given during client init.

        Parameters
        ----------
        topic_name : str
            The name of new topic to be created.

        Returns
        -------
        None
        """
        try:
            self._publisher.create_topic(request={f"{topic_name}": self._topic_path})
            self._logger.info(
                f"New topic named: {topic_name}, created at: {self._topic_path}."
            )
        except Exception as e:
            self._logger.error(
                f"Error! There was a problem while attempting to create new topic: {e}"
            )
        self._logger.info("Complete.")

    def delete_topic(self, topic_name: str) -> None:
        """
        Deletes an existing topic of the given name, uses topic_path given during client init.

        Parameters
        ----------
        topic_name : str
            The name of topic to be deleted.

        Returns
        -------
        None
        """
        try:
            topic_path = self._topic_path
            self._publisher.delete_topic(request={"topic": topic_path})
            self._logger.info(
                f"{topic_name} has been deleted successfully from {topic_path}"
            )
        except Exception as e:
            self._logger.error(
                f"Error! There was a problem while attempting to delete {topic_name}: {e}"
            )
        self._logger.info("Complete.")

    def list_subs_in_topic(self, local: bool = False) -> str:
        """
        Lists subscriptions for a given topic. Uses topic provided upon initializing the client.

        Parameters
        ----------
        local : bool
            If testing, set this to true so results are printed in console/ipynb rather than into logs.
            Default is False.

        Returns
        -------
        str
            Returns a string of the subscriptions for the given topic.
        """
        if local == True:
            for subscription in self._publisher.list_topic_subscriptions(
                self._topic_path
            ):
                print(subscription)
        else:
            for subscription in self._publisher.list_topic_subscriptions(
                self._topic_path
            ):
                self._logger.info(subscription)

    def create_subscription(
        self,
        subscription: str,
        push_config: pubsub_v1.types.PushConfig,
        ack_deadline_seconds: int,
    ) -> None:
        """
        Creates a new pull sub in the current topic.

        Parameters
        ----------
        subscription : str
            The subscription name.
        push_config : pubsub_v1.types.PushConfig
            Modifies the push configuration, currently the endpoint is declared during client init.
        ack_deadline_seconds : int
            The deadline (in seconds) for sub acknowledgement.

        Returns
        -------
        None
        """
        try:
            subscription_path = self._subscriber.subscription_path(
                self._project_id, subscription
            )
            # https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.client.Client#google_cloud_pubsub_v1_subscriber_client_Client_create_subscription
            # add in push_config, ack_deadline_seconds, retry,
            subscription = self._subscriber.create_subscription(
                subscription_path, self._topic_path, push_config, ack_deadline_seconds
            )
            self._logger.info(f"Subscription created: {subscription}")
        except Exception as e:
            self._logger.error(
                f"Error! There was a problem while attempting to create subscription: {e}"
            )
        self._logger.info("Complete.")

    def delete_subscription(self, subscription_name: str) -> None:
        """
        Deletes an existing Pub/Sub subscription.

        Parameters
        ----------
        subscription_name : str
            The name of subscription to be deleted.

        Returns
        -------
        None
        """
        try:
            subscription_path = self._subscriber.subscription_path(
                self._project_id, subscription_name
            )
            self._subscriber.delete_subscription(subscription_path)
            self._logger.info(f"Subscription deleted: {subscription_path}")
        except Exception as e:
            self._logger.error(
                f"Error! There was a problem while attempting to delete subscription: {e}"
            )
        self._logger.info("Complete.")
