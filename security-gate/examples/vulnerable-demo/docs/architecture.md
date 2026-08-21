# Telemetry Ingest — Architecture (fixture)

A deliberately incomplete architecture document, used to exercise the MARKNA
architecture checklist. It mentions some topics and omits others on purpose.

## Overview

Devices publish telemetry to a collector. The collector normalises the payload
and writes it to a local database. A small web UI renders the last 24 hours.

## Components

- **Collector** — receives device payloads over http://collector.example.com/ingest
  and writes to SQLite.
- **Web UI** — a Flask application. For the UAT window it is publicly accessible
  so the client can try it from their own network.
- **Device firmware** — ESP32 units on the customer LAN.

## Deployment

The collector and the UI both run on the same host and bind 0.0.0.0. The service
runs as root because it needs to write to /var/lib.

## Authentication

There is no authentication on the collector endpoint during UAT; devices are
identified by a shared device key embedded in the firmware. TLS is TBD.

## Data

Telemetry is written to a local SQLite file. Backups are not configured yet.

## Monitoring

Application logs are written to /var/log/telemetry.log on the host.
