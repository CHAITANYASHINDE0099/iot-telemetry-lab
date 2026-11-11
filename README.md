# IoT Telemetry Lab

Simulate temperature and humidity sensors, send readings over MQTT, and collect them in SQLite. The collector validates each message, ignores duplicate reading IDs, and flags readings at or above 30 °C or 75% relative humidity.

## Try the offline demo

Use Python 3.11 or newer.

~~~bash
python telemetry.py --db ":memory:" demo --count 10
~~~

The demo uses the same validation and SQLite collector as the MQTT listener. A fresh run reports 10 stored readings, two temperature alerts, and `"duplicate_inserted": false`.

## Run over MQTT

Install the Python client and start the local broker:

~~~bash
python -m pip install -r requirements.txt
docker compose up -d
~~~

In one terminal, start the collector:

~~~bash
python telemetry.py listen
~~~

In another terminal, publish readings and inspect the database:

~~~bash
python telemetry.py publish --device room-01 --count 10
python telemetry.py report
~~~

Stop the collector with Ctrl+C and the broker with `docker compose down`. The sample Docker Compose broker accepts local connections on `127.0.0.1:1883`. To connect to an existing TLS broker, set `--host`, `--port`, and `--ca-file` on both commands. A broker username can be supplied with `--username` and its password through `MQTT_PASSWORD`.

## Connect a device

Publish JSON to `lab/sensors/{device_id}/telemetry` with QoS 1. Device IDs in the topic and body must match. Use a new UUID for each reading and reuse that UUID when retrying the same reading.

~~~json
{
  "reading_id": "f5cb40f1-879f-4f56-a373-59338f2c132e",
  "device_id": "room-01",
  "captured_at": "2026-01-01T10:00:00Z",
  "temperature_c": 31.2,
  "humidity_pct": 58.4
}
~~~

References [Eclipse Paho Python client](https://eclipse.dev/paho/clients/python/docs/) and [Mosquitto Docker image](https://hub.docker.com/_/eclipse-mosquitto).
