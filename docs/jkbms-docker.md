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

## Check

```bash
docker compose -f compose.jkbms.yaml -p jkbms ps
docker compose -f compose.jkbms.yaml -p jkbms logs -f jkbms_api

curl -sS http://127.0.0.1:5005/health | python3 -m json.tool
curl -sS http://127.0.0.1:5005/bms    | python3 -m json.tool
```

## Stop (leaves production alone)

```bash
docker compose -f compose.jkbms.yaml -p jkbms down
```

## Notes

- `network_mode: host` + `/var/run/dbus` is required for reliable BLE on this Pi.
- Config: edit `jkbms.yaml` on the host (bind-mounted read-only).
- Production compose project must keep running independently; do not merge
  these services into production `compose.yaml` until soak-tested.
