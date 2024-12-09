# wbor-groupme-producer

Heavily WIP

Docker container that serves to publish GroupMe messages to our RabbitMQ exchange.

* Local API that the producing party (e.g. ENDEC Newsfeed, `apcupsd`) uses to make the request.
* If the exchange doesn't respond with success, fall back to own interface with the GroupMe API and attempt to send a message from that. Records logs of these events and puts in a queue to send to our MQ exchange to process these logs in Postgres (central source of truth for all API events with GroupMe).
