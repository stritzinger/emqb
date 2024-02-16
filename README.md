emqb
=====

Erlang MQTT bridge

Overview
--------

It offers an interface similar to `emqtt`, but allows the clients running in the
same VM to comunicate directly without waiting for the MQTT broker messages to
get back.

Could be used in multiple mode:

 - `internal` : All the messages are only local to the VM. This allow multiple
   erlang services in a same VM to use MQTT topics between them without any
   external MQTT broker.
 - `external` : All the messages are sent exclusively to an external MQTT broker.
   There is no much difference between this mode and using `emqtt` directly.
 - `hybride` : In this mode, the messages are sent both internally and to the
   external broker. The messages are taged with an instance identifier so they
   are filtered out when comming back to the VM. This allows all the services
   in the VM to comunicate directly while any external services using the same
   broker can still receive the messages too.

Only supports MQTT version 5 brokers.

Currently not supporting QoS 2 and QoS 1 is pretty much simulated when
messages are delivered internally.

Currently not supporting retained messages internally.

Implement automatic reconnection to the broker, with exponential backoff.


Differences with EMQTT
----------------------

The main difference between `emqtt` and `emqb` are:

 - `emqb` payloads are erlang terms, the encoding and decoding is done on-demand
   by a callback module given as an option when starting the client. This way
   not encoding or decoding is required when the message is not sent to the MQTT
   broker.
 - `emqb` does not support QoS 2 at this moment.
 - `emqb` `start_link` functions do not return a sinple PID, but an opaque
   data structure. It means it cannot be used in a supervisor, but that would
   be weird to use it like this anyway.
 - `emqb` do not have `connect` and `disconnect` functions, as it handles
   automatic reconnection with exponential backoff.
 - The packet identifier in the received messages can either be an integer if
   the message came from an external MQTT broker, or an erlang reference if
   it was delivered internally.
 

Bypass in Hybrid Mode
---------------------

If an internal service is publishing a message, and is sure only a single
consumer should be subscribed to the topic, it can specify the publishing
option `{bypass, true}`. With this option, if there is an internal subscriber
to the topic, the message is **only** sent internally, it is **not** sent to
the external MQTT broker. If there is no local subscriber, the message will
still be sent to the broker. This could reduce the messages going out to the
broker, but this should be used carefully.


Build
-----

    $ rebar3 compile


Tests
-----

To run the tests that needs a real MQTT broker, the following environment
variables may be set:

  - `MQTT_BROKER_HOST`: If not set "localhost" is assumed.
  - `MQTT_BROKER_PORT`: If not set, 1883 is assumed.
  - `MQTT_BROKER_USERNAME`: If not set, no authentication is done.
  - `MQTT_BROKER_PASSWORD`: If not set, no authentication is done.

Then you can run the tests with:

    $ rebar3 ct

And see the report in `_build/test/logs/last/index.html`.
