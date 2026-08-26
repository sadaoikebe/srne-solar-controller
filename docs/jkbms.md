# JK-BMS monitoring

Production path for dual JK-BMS packs over Bluetooth LE (read-only).

## Services

| Service | Role |
|---------|------|
| `jkbms_api` | Per-bank keep-alive BLE poll (~10 s), HTTP cache on port **5005** |
| `jkbms_db_writer` | Writes measurement **`jkbms`** to Influx every 30 s |

Both use image `srne-app:latest` and `network_mode: host` (BlueZ / D-Bus). Configured in `compose.yaml`.

## Config

- Bank MAC / serial map: `jkbms.yaml`
- `poll_interval_s` (default 10): per-bank keep-alive query interval
- `read_timeout_s` (default 8): wait for a cell-info notify after `0x96`
- Env: `JKBMS_API_PORT` (default 5005), Influx vars shared with the rest of the stack

Each bank has its own poll task. Scan+connect is serialized on the single
`hci0` radio; a timeout on one bank does not stall the other. Influx writes
stay at 30 s (`jkbms_db_writer`).

## Safety

- Only BLE **query** frames (`0x96` / `0x97`) — no charge/discharge/balance/settings writes
- Phone app and Pi cannot share a BMS connection (single BLE client)

## Ops

```bash
curl -s http://127.0.0.1:5005/health | python3 -m json.tool
curl -s http://127.0.0.1:5005/bms    | python3 -m json.tool
docker logs -f jkbms_api
docker logs -f jkbms_db_writer
```

Grafana: **JK-BMS Battery Banks** → `/d/solar-jkbms/jk-bms-battery-banks`

## Design history

The original multi-step design and parallel-stack notes lived in
`docs/jkbms-implementation-plan.md` (removed after production merge). Full text
remains in git history, e.g.:

```bash
git log --all -- docs/jkbms-implementation-plan.md
git show <commit>:docs/jkbms-implementation-plan.md
```
