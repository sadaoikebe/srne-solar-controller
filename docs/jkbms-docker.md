# jkbms_api — parallel Docker run

Isolated from the production solar stack (`modbus_api`, `db_writer`, …).

## Prerequisites

- Host Bluetooth up: `systemctl status bluetooth`
- Phone JK-BMS app **disconnected** (single BLE client)
- Docker / Compose available
- Free host port **5005** (production Modbus API uses **5004**)

## Start

From this worktree:

```bash
cd ~/srne-solar-controller-jkbms
docker compose -f compose.jkbms.yaml -p jkbms up -d --build
```

## Secrets / env

Compose loads a local `.env` (gitignored). For host-network Influx access:

```bash
# Example: copy tokens from production, override URL for host network
grep -E '^(TZ|LOG_LEVEL|INFLUX_ORG|INFLUX_BUCKET|INFLUX_TOKEN)=' \
  ../srne-solar-controller/.env > .env
echo 'INFLUX_URL_HOST=http://127.0.0.1:8086' >> .env
echo 'JKBMS_API_URL=http://127.0.0.1:5005/bms' >> .env
```

## Check

```bash
docker compose -f compose.jkbms.yaml -p jkbms ps
docker compose -f compose.jkbms.yaml -p jkbms logs -f jkbms_api
docker compose -f compose.jkbms.yaml -p jkbms logs -f jkbms_db_writer

curl -sS http://127.0.0.1:5005/health | python3 -m json.tool
curl -sS http://127.0.0.1:5005/bms    | python3 -m json.tool
```

Writer should log: `Wrote 72 jkbms points to InfluxDB …`. Query measurement
`jkbms` in the production bucket (Data Explorer or `influx query`) — no Grafana required.

## Stop (leaves production alone)

```bash
docker compose -f compose.jkbms.yaml -p jkbms down
```

## Notes

- `network_mode: host` + `/var/run/dbus` is required for reliable BLE on this Pi.
- Config: edit `jkbms.yaml` on the host (bind-mounted read-only).
- Production compose project must keep running independently; do not merge
  these services into production `compose.yaml` until soak-tested.
