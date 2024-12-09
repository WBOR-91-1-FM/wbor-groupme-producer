"""
Producer
"""

import logging
import json
import sys
import os
import signal
from datetime import datetime, timezone
from uuid import uuid4
import pika
from pika.exceptions import (
    AMQPConnectionError,
    AMQPChannelError,
)
from flask import Flask, abort, request
from utils.logging import configure_logging
from config import (
    APP_PORT,
    APP_PASSWORD,
    SOURCE,
    RABBITMQ_HOST,
    RABBITMQ_USER,
    RABBITMQ_PASS,
    RABBITMQ_EXCHANGE,
)

logging.root.handlers = []
logger = configure_logging()

app = Flask(__name__)


def terminate(exit_code=1):
    """Terminate the process."""
    os.kill(os.getppid(), signal.SIGTERM)  # Gunicorn master
    os._exit(exit_code)  # Current thread


def publish_to_exchange(key, sub_key, data):
    """
    Publishes a message to a RabbitMQ exchange.

    Parameters:
    - key (str): The name of the message key.
    - sub_key (str): The name of the sub-key for the message. (e.g. 'sms', 'call')
    - data (dict): The message content, which will be converted to JSON format.
    """
    try:
        logger.debug("Attempting to connect to RabbitMQ...")
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=5672,
            credentials=credentials,
            client_properties={"connection_name": "GroupMeProducerConnection"},
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        logger.debug("RabbitMQ connected!")

        # Assert the exchange exists
        channel.exchange_declare(
            exchange=RABBITMQ_EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        # Publish message to exchange
        channel.basic_publish(
            exchange=RABBITMQ_EXCHANGE,
            routing_key=f"source.{key}.{sub_key}",
            body=json.dumps(
                {**data, "type": sub_key}  # Include type in the message body
            ).encode(),  # Encodes msg as bytes. RabbitMQ requires byte data
            properties=pika.BasicProperties(
                headers={
                    "x-retry-count": 0
                },  # Initialize retry count for other consumers
                delivery_mode=2,  # Persistent message - write to disk for safety
            ),
        )
        logger.debug(
            "Publishing message body: %s", json.dumps({**data, "type": sub_key})
        )
        logger.info(
            "Published message to `%s` with routing key: `source.%s.%s`: %s - UID: %s",
            RABBITMQ_EXCHANGE,
            key,
            sub_key,
            data.get("message"),
            data.get("wbor_message_id"),
        )
        connection.close()
    except AMQPConnectionError as conn_error:
        error_message = str(conn_error)
        logger.error(
            "Connection error when publishing to `%s` with routing key "
            "`source.%s.%s`: %s",
            RABBITMQ_EXCHANGE,
            key,
            sub_key,
            error_message,
        )
        if "CONNECTION_FORCED" in error_message and "shutdown" in error_message:
            logger.critical("Broker shut down the connection. Shutting down consumer.")
            sys.exit(1)
        if "ACCESS_REFUSED" in error_message:
            logger.critical(
                "Access refused. Check RabbitMQ user permissions. Shutting down consumer."
            )
        terminate()
    except AMQPChannelError as chan_error:
        logger.error(
            "Channel error when publishing to `%s` with routing key `source.%s.%s`: %s",
            RABBITMQ_EXCHANGE,
            key,
            sub_key,
            chan_error,
        )
    except json.JSONDecodeError as json_error:
        logger.error("JSON encoding error for message %s: %s", data, json_error)


@app.route("/send", methods=["GET"])
def publish_message():
    """
    Send a message from a browser address bar. Requires a password.
    """
    logger.info("Received request to send message from browser...")
    # Don't let strangers send messages as if they were us!
    password = request.args.get("password")
    if password != APP_PASSWORD:
        abort(403, "Unauthorized access")

    message = request.args.get("body")

    if not message:
        logger.warning("Message body content missing")
        abort(400, "Message body text is required")

    # Queue the message for sending
    message_id = str(uuid4())  # Generate a unique ID for tracking
    outgoing_message = {
        "wbor_message_id": message_id,
        "body": message,
        "source": request.args.get("source", SOURCE),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    publish_to_exchange(SOURCE, "test", outgoing_message)
    logger.info("Message queued for sending. UID: %s", message_id)
    return "Message queued for sending"


@app.route("/")
def hello_world():
    """Serve a simple static Hello World page at the root"""
    return "<h1>wbor-groupme-producer is online!</h1>"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APP_PORT)
