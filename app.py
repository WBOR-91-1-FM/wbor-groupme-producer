"""
Producer
"""

import logging
import json
import sys
import os
import signal
import threading
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


def publish_to_exchange(key, sub_key, data, alreadysent=False):
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

        # Declare a temporary queue for acknowledgment
        result = channel.queue_declare(queue="", exclusive=True)
        callback_queue = result.method.queue
        correlation_id = data.get("wbor_message_id") or str(uuid4())

        # Set headers for message
        headers = {"x-retry-count": 0}  # Initialize retry count for other consumers
        if alreadysent:
            headers["alreadysent"] = True

        # Publish message to exchange
        channel.basic_publish(
            exchange=RABBITMQ_EXCHANGE,
            routing_key=f"source.{key}.{sub_key}",
            body=json.dumps(
                {**data, "type": sub_key}  # Include type in the message body
            ).encode(),  # Encodes msg as bytes. RabbitMQ requires byte data
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=correlation_id,
                headers=headers,
                delivery_mode=2,  # Persistent message - write to disk for safety
            ),
        )
        logger.debug(
            "Publishing message body with Correlation ID `%s`: `%s`",
            correlation_id,
            json.dumps({**data, "type": sub_key}),
        )
        logger.info(
            "Published message to `%s` (corr: `%s`) with routing key: `source.%s.%s`: `%s` - `%s`",
            RABBITMQ_EXCHANGE,
            correlation_id,
            key,
            sub_key,
            data.get("text"),
            data.get("wbor_message_id"),
        )

        ack_received = threading.Event()

        def on_ack(ch, _method, properties, body):
            if properties.correlation_id == correlation_id:
                logger.debug(
                    "Acknowledgment received for message with Correlation ID `%s`: `%s`",
                    correlation_id,
                    body.decode(),
                )
                ack_received.set()
                ch.stop_consuming()

        # Listen for acknowledgment
        channel.basic_consume(
            queue=callback_queue, on_message_callback=on_ack, auto_ack=True
        )

        # Start consuming in a separate thread
        def consume():
            channel.start_consuming()

        consumer_thread = threading.Thread(target=consume)
        consumer_thread.start()

        # Wait for acknowledgment with a timeout
        if not ack_received.wait(timeout=5):  # 5-second timeout
            logger.warning(
                "No acknowledgment received for message with Correlation ID `%s` within 5 seconds",
                correlation_id,
            )
            # TODO: Take action, e.g., retry and add failure log to queue
            logger.critical("Message delivery failed.")
            ack_received.set()  # Ensure the consumer thread stops
            channel.stop_consuming()

        consumer_thread.join()
        connection.close()
    except AMQPConnectionError as conn_error:
        error_message = str(conn_error)
        logger.error(
            "Connection error when publishing to `%s` with routing key "
            "`source.%s.%s`: `%s`",
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
            "Channel error when publishing to `%s` with routing key `source.%s.%s`: `%s`",
            RABBITMQ_EXCHANGE,
            key,
            sub_key,
            chan_error,
        )
    except json.JSONDecodeError as json_error:
        logger.error("JSON encoding error for message %s: %s", data, json_error)


@app.route("/send", methods=["POST"])
def publish_message():
    """
    Send a message from a POST request.

    Expects a valid JSON payload with the following:
    - password: The password for the application (defined by APP_PASSWORD)
    - text: The message body content
    - bot_id: The bot ID the message was sent from
    - statuscode: The status code for the message
    - source: The source of the message
    - alreadysent: A boolean indicating if the message has already been sent
    """
    payload = request.json
    logger.info("Received request to send message with payload %s", payload)
    if not payload:
        logger.warning("Request payload missing or not in JSON format")
        abort(400, "Request must contain a valid JSON payload")

    # Don't let strangers send messages as if they were us!
    password = payload.get("password")
    if password != APP_PASSWORD:
        abort(403, "Unauthorized access")

    message = payload.get("text")
    bot_id = payload.get("bot_id", "")

    if not message:
        logger.warning("Message body content missing")
        abort(400, "Message body text is required")

    # Queue the message for sending
    message_id = str(uuid4())  # Generate a unique ID for tracking
    outgoing_message = {
        "wbor_message_id": message_id,
        "text": message,
        "statuscode": payload.get("statuscode"),
        "bot_id": bot_id,
        "source": payload.get("source", SOURCE),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    already_sent = payload.get("alreadysent", False)

    # If `alreadysent` is true, `statuscode` must be provided
    code = payload.get("statuscode")
    if already_sent and not code:
        logger.warning("Message already sent but no code provided")
        abort(400, "Code is required if message has already been sent")

    publish_to_exchange(SOURCE, "test", outgoing_message, alreadysent=already_sent)
    logger.info("Message sent!: `%s`", message_id)
    return "Message sent!"


@app.route("/")
def hello_world():
    """Serve a simple static Hello World page at the root"""
    return "<h1>wbor-groupme-producer is online!</h1>"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APP_PORT)
