"""Simulate, collect, and inspect temperature and humidity sensor readings."""

import argparse
import json
import os
import random
import re
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4


TOPIC_PREFIX = "lab/sensors"
DEVICE_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,39}")
TEMP_ALERT_C = 30.0
HUMIDITY_ALERT_PERCENT = 75.0


def device_id(value):
    if not isinstance(value, str) or not DEVICE_ID.fullmatch(value):
        raise ValueError("device_id must be 1 to 40 lowercase letters, digits, or hyphens")
    return value


def number(value, name, low, high):
    if type(value) not in (float, int) or not low <= value <= high:
        raise ValueError(f"{name} must be a number between {low} and {high}")
    return float(value)


@dataclass(frozen=True)
class Reading:
    reading_id: str
    device_id: str
    captured_at: str
    temperature_c: float
    humidity_pct: float

    @property
    def alert(self):
        return (
            self.temperature_c >= TEMP_ALERT_C or self.humidity_pct >= HUMIDITY_ALERT_PERCENT
        )

    @classmethod
    def decode(cls, payload):
        if len(payload) > 4096:
            raise ValueError("Telemetry payload exceeds 4 KB")
        try:
            data = json.loads(payload)
            if not isinstance(data, dict):
                raise ValueError("Expected a JSON object")
            reading_id = str(UUID(data["reading_id"]))
            when = datetime.fromisoformat(data["captured_at"])
            if when.utcoffset() is None:
                raise ValueError("captured_at must include a timezone")
            return cls(
                reading_id=reading_id,
                device_id=device_id(data["device_id"]),
                captured_at=when.astimezone(UTC).isoformat(timespec="microseconds"),
                temperature_c=number(data["temperature_c"], "temperature_c", -40, 120),
                humidity_pct=number(data["humidity_pct"], "humidity_pct", 0, 100),
            )
        except (KeyError, TypeError, AttributeError, UnicodeError, OverflowError) as exc:
            raise ValueError("Invalid telemetry JSON or missing fields") from exc


def topic(device):
    return f"{TOPIC_PREFIX}/{device}/telemetry"


def encode(reading):
    return json.dumps(asdict(reading), separators=(",", ":")).encode("utf-8")


def open_database(path):
    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute(
        """CREATE TABLE IF NOT EXISTS readings (
            reading_id TEXT PRIMARY KEY, device_id TEXT NOT NULL, captured_at TEXT NOT NULL,
            temperature_c REAL NOT NULL, humidity_pct REAL NOT NULL, alert INTEGER NOT NULL
        )"""
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS readings_by_device_time ON readings(device_id, captured_at)"
    )
    return connection


def ingest(connection, message_topic, payload):
    reading = Reading.decode(payload)
    if message_topic != topic(reading.device_id):
        raise ValueError("Topic does not match the payload device_id")
    with connection:
        result = connection.execute(
            """INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(reading_id) DO NOTHING""",
            (
                reading.reading_id, reading.device_id, reading.captured_at,
                reading.temperature_c, reading.humidity_pct, reading.alert,
            ),
        )
    return reading, result.rowcount == 1


def report(connection):
    rows = connection.execute(
        """SELECT device_id, COUNT(*) AS readings, SUM(alert) AS alerts,
                  ROUND(AVG(temperature_c), 2) AS average_c, MAX(captured_at) AS last_seen
           FROM readings GROUP BY device_id ORDER BY device_id"""
    )
    return [dict(row) for row in rows]


def simulate(device, count, seed):
    rng = random.Random(seed)
    for index in range(count):
        temperature = rng.uniform(23, 29)
        if index % 5 == 4:
            temperature = rng.uniform(31, 35)
        yield Reading(
            reading_id=str(uuid4()), device_id=device,
            captured_at=datetime.now(UTC).isoformat(timespec="microseconds"),
            temperature_c=round(temperature, 2), humidity_pct=round(rng.uniform(42, 70), 2),
        )


def make_client(args):
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:
        raise ValueError("Install MQTT support with python -m pip install -r requirements.txt") from exc
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
    if args.ca_file:
        client.tls_set(ca_certs=str(args.ca_file))
    if args.username:
        client.username_pw_set(args.username, os.environ.get("MQTT_PASSWORD"))
    return client


def listen(args, connection):
    client = make_client(args)
    refusal = []

    def on_connect(connected_client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            refusal.append(str(reason_code))
            connected_client.disconnect()
            return
        connected_client.subscribe(topic("+"), qos=1)

    def on_subscribe(connected_client, userdata, mid, reason_codes, properties):
        if any(code.is_failure for code in reason_codes):
            refusal.append("The broker refused the telemetry subscription")
            connected_client.disconnect()
            return
        print(f"Listening on {topic('+')}", flush=True)

    def on_message(connected_client, userdata, message):
        try:
            reading, inserted = ingest(connection, message.topic, message.payload)
        except ValueError as exc:
            print(f"Discarded invalid reading: {exc}", file=sys.stderr, flush=True)
            return
        if inserted:
            print(json.dumps({**asdict(reading), "alert": reading.alert}), flush=True)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect(args.host, args.port, keepalive=30)
    try:
        client.loop_forever()
    finally:
        client.disconnect()
    if refusal:
        raise ValueError(refusal[0])


def publish(args):
    client = make_client(args)
    ready = Event()
    result = []

    def on_connect(connected_client, userdata, flags, reason_code, properties):
        result.append(reason_code)
        ready.set()

    client.on_connect = on_connect
    client.connect(args.host, args.port, keepalive=30)
    client.loop_start()
    try:
        if not ready.wait(timeout=8):
            raise ValueError("Timed out waiting for MQTT connection acknowledgment")
        if result[0].is_failure:
            raise ValueError(f"Broker refused the connection: {result[0]}")
        for reading in simulate(args.device, args.count, args.seed):
            message = client.publish(topic(reading.device_id), encode(reading), qos=1)
            message.wait_for_publish(timeout=5)
            if not message.is_published():
                raise ValueError(f"Broker did not acknowledge {reading.reading_id}")
        print(f"Published {args.count} readings to {topic(args.device)}")
    finally:
        client.disconnect()
        client.loop_stop()


def offline_demo(args, connection):
    first_payload = None
    for reading in simulate(args.device, args.count, args.seed):
        payload = encode(reading)
        ingest(connection, topic(args.device), payload)
        if first_payload is None:
            first_payload = payload
    _, inserted_twice = ingest(connection, topic(args.device), first_payload)
    return {"duplicate_inserted": inserted_twice, "devices": report(connection)}


def with_database(args):
    connection = open_database(args.db)
    try:
        if args.command == "listen":
            listen(args, connection)
        else:
            result = offline_demo(args, connection) if args.command == "demo" else report(connection)
            print(json.dumps(result, indent=2))
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("telemetry.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Run through the collector without a broker")
    send = commands.add_parser("publish", help="Publish simulated readings over MQTT")
    receive = commands.add_parser("listen", help="Collect live MQTT readings")
    commands.add_parser("report", help="Summarize the SQLite readings by device")
    for command in (demo, send):
        command.add_argument("--device", default="room-01")
        command.add_argument("--count", type=int, default=10)
        command.add_argument("--seed", type=int, default=42)
    for command in (send, receive):
        command.add_argument("--host", default="127.0.0.1")
        command.add_argument("--port", type=int, default=1883)
        command.add_argument("--username")
        command.add_argument("--ca-file", type=Path, help="Broker TLS CA certificate")
    args = parser.parse_args()

    try:
        if args.command in ("demo", "publish"):
            device_id(args.device)
            if not 1 <= args.count <= 10000:
                raise ValueError("--count must be between 1 and 10000")
        if args.command == "publish":
            publish(args)
            return
        with_database(args)
    except KeyboardInterrupt:
        return
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
