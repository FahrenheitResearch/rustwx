# HRRR MDT/HIGH Sounding Archive MVP

This archive lane is for event-scoped HRRR pressure-volume stores behind WxStore. The first target is the SPC HIGH-day subset, then MDT+ once the ingest and viewer shape are stable.

## Inputs

The ingest CLI reads Claude's event planning dataset:

- `mdt_plus_dates.json`
- `archive_plan_v2.json`
- `mrgl_areas.json`

Local default: `C:/Users/drew/tor-sound-arch/dataset`.

Node default: copy those files under `/data/weather-api/archive/dataset` and pass that path as `--dataset-root`.

## Storage Layout

Archive metadata:

```text
/data/weather-api/archive/
  events/{YYYY-MM-DD}/event.json
  events/{YYYY-MM-DD}/manifest.json
  events/{YYYY-MM-DD}/mrgl.geojson
  events/{YYYY-MM-DD}/volume_mask.geojson
  ingest/checkpoints/{YYYY-MM-DD}/{run_id}.json
  ingest/last_plan_summary.json
  ingest/last_execute_summary.json
```

Pressure-volume stores:

```text
/data/weather-api/pressure_volume/
  hrrr_archive/{run_id}/store/manifest.json
  hrrr_archive/{run_id}/store/index.bin
  hrrr_archive/{run_id}/store/chunks.bin
```

Run IDs are event-scoped:

```text
{convective_day}_{synoptic|near}_{YYYYMMDD}_{CC}z
```

Example:

```text
2024-05-06_near_20240506_06z
```

## Ingest CLI

Dry-run one HIGH event:

```bash
hrrr_mdt_archive_ingest \
  --dataset-root /data/weather-api/archive/dataset \
  --archive-root /data/weather-api/archive \
  --pressure-volume-root /data/weather-api/pressure_volume \
  --cache-dir /data/weather-api/cache \
  --rank high \
  --event 2024-05-06 \
  --kind near
```

Build the near-event pressure store for that event:

```bash
hrrr_mdt_archive_ingest \
  --dataset-root /data/weather-api/archive/dataset \
  --archive-root /data/weather-api/archive \
  --pressure-volume-root /data/weather-api/pressure_volume \
  --cache-dir /data/weather-api/cache \
  --rank high \
  --event 2024-05-06 \
  --kind near \
  --execute
```

Useful knobs:

- `--rank high` for the 11 HIGH days.
- `--rank mdt-plus` for all 128 MDT/HIGH days.
- `--kind near` for the day-of/near-event 06z run.
- `--kind synoptic` for the earlier synoptic 06z run.
- `--pressure-hours sounding` for the planned 12-hour sounding window.
- `--pressure-hours plot` for every plot hour in the run.
- `--pressure-hours both` for both sets.
- `--buffer-fraction` and `--buffer-km` control the MRGL bounding-box expansion.

## WxStore Routes

Run WxStore with an archive root:

```bash
wxstore serve \
  --profile-store /data/weather-api/profile \
  --spatial-root /data/weather-api/wxstore_spatial \
  --static-plots-root /data/weather-api/static \
  --archive-root /data/weather-api/archive \
  --host 127.0.0.1 \
  --port 18080
```

Archive endpoints:

```text
GET /hrrrarchive
GET /v1/archive/status
GET /v1/archive/events?rank=high
GET /v1/archive/events/{YYYY-MM-DD}
GET /v1/archive/events/{YYYY-MM-DD}/polygons
GET /v1/archive/events/{YYYY-MM-DD}/runs
```

The `/archive` and `/v1/archive/...` paths remain aliases. Public links should use `/hrrrarchive` and `/v1/hrrrarchive/...`.

Cross sections use the existing route with:

```json
{
  "model": "hrrr_archive",
  "run": "2024-05-06_near_20240506_06z"
}
```

## MVP Scope

This first slice stores pressure-volume data for arbitrary soundings and cross sections. It does not yet store the full Tier A severe scalar grid or ECAPE maps. Those should be added as a second archive store beside the pressure-volume store once the HIGH-day pressure path is validated.
