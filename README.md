[![hacs_badge](https://img.shields.io/badge/HACS-Default-41BDF5.svg)](https://github.com/hacs/integration) [![pre-commit.ci status](https://results.pre-commit.ci/badge/github/stackia/ha-deye-dehumidifier/main.svg)](https://results.pre-commit.ci/latest/github/stackia/ha-deye-dehumidifier/main)

# Home Assistant Integration for Deye Dehumidifiers

This is a custom component for Home Assistant to integrate [Deye](https://deye.com/) dehumidifiers.

![screenshot](https://raw.githubusercontent.com/stackia/ha-deye-dehumidifier/main/screenshot.png)

## Installation

You can install using [HACS](https://hacs.xyz/).

1. Make sure you have correctly installed HACS.
2. Search `Deye Dehumidifier` in HACS.
3. Setup the integration by typing your username (phone number) and password of the Deye app.

![hacs-guide](https://raw.githubusercontent.com/stackia/ha-deye-dehumidifier/main/hacs-guide.png)

## Supported Models

See <https://github.com/stackia/libdeye#libdeye>.

## State updates and diagnostics

The integration polls configured devices every 30 seconds. Fog devices are asked
to report their current state, then their properties are read over HTTP; receiving
an MQTT reply is not required. Classic and Combo devices reply to queries over MQTT.
Commands and online notifications do not postpone these periodic updates.
Empty or control-only Fog property packets do not count as new measurements.
Partial measurement packets preserve the last readings for omitted fields.
Newer MQTT readings do not prevent other measurements from updating over HTTP.
Humidity and temperature each expire after three minutes without that particular
measurement. The other sensor and device controls remain available when their
data is still fresh. A new reading automatically restores the expired sensor.

After three minutes without a valid state payload, cached readings are marked
unavailable until state reception resumes. Brief HTTP outages keep the previous
readings during this grace period. Successful commands and online notifications
do not make cached temperature or humidity fresh again. Classic/Combo devices
also recover missing MQTT replies by reloading the account, with a ten-minute
cooldown between automatic reloads.

For troubleshooting, download the integration diagnostics in Home Assistant and
check `last_state_received`, `state_data_stale`, `state_query_failures`, and
`mqtt_connected`. `last_state_received` records when Home Assistant received a
payload; it is not a timestamp of the physical measurement. Unchanged sensor
values can still be receiving valid updates.
For partial reports, `last_measurement_received` and `measurement_data_stale`
show freshness separately for humidity and temperature.

Temporary HTTP timeouts (408), rate limits (429), and server errors (5xx) are retried. Requests to sign in
again are reserved for responses the SDK identifies as authentication failures.
After a 429, device polls, commands, and MQTT credential requests for the same
account honor the shared `Retry-After` cooldown, including across integration
reloads. Requests resume after the cooldown; other accounts are unaffected.
