# earthranger-bigquery-pipeline

A resumable ELT pipeline that incrementally syncs [EarthRanger](https://www.earthranger.com/) data into [Google BigQuery](https://cloud.google.com/bigquery) for analysis, reporting, and long-term archiving.

Built for conservation operations teams that need their EarthRanger data in a queryable data warehouse — for patrol analytics, incident reporting, and population monitoring.

## What it syncs

| Task | EarthRanger endpoint | BigQuery table | Mode |
|---|---|---|---|
| `observations` | `/observations/` | `raw_er_observations` | Incremental (cursor) |
| `events` | `/activity/events/` | `raw_er_events` | Incremental (cursor) |
| `patrols` | `/activity/patrols/` | `raw_er_patrols` | Incremental (cursor) |
| `snapshot_all` | subjects, sources, users, event types, choices, assignments | `raw_er_*` | Full snapshot |

## Architecture

```
Cloud Run Job (--task flag) → EarthRanger REST API → BigQuery (streaming insert)
                                     ↕
                           BigQuery elt_state table (cursor persistence)
```

Deployed as a Google Cloud Run **Job** (not a service). Each task is a separate job execution with a `--task` argument.

## Setup

### 1. BigQuery dataset

Create the dataset and the cursor table:

```sql
CREATE TABLE `your-project.earthranger_data_v7.elt_state` (
  pipeline_key_pk STRING NOT NULL,
  cursor_value STRING,
  last_updated_ts TIMESTAMP
);
```

### 2. Deploy to Cloud Run Jobs

```bash
gcloud run jobs deploy er-pipeline-v7-job \
  --source . \
  --region us-central1 \
  --set-env-vars GCP_PROJECT_ID=your-project-id \
  --set-env-vars ER_SITE=https://your-site.pamdas.org \
  --set-env-vars EARTHRANGER_TOKEN=your_token \
  --set-env-vars BQ_DATASET_ID=earthranger_data_v7 \
  --memory 2Gi \
  --task-timeout 3600
```

### 3. Run a task

```bash
# Incremental sync
gcloud run jobs execute er-pipeline-v7-job --args="--task,observations"
gcloud run jobs execute er-pipeline-v7-job --args="--task,events"
gcloud run jobs execute er-pipeline-v7-job --args="--task,patrols"

# Full snapshot of reference tables
gcloud run jobs execute er-pipeline-v7-job --args="--task,snapshot_all"
```

### 4. Schedule with Cloud Scheduler

Recommended schedule: run each incremental task every 30–60 minutes, `snapshot_all` once daily.

## Environment Variables

| Variable | Description |
|---|---|
| `GCP_PROJECT_ID` | GCP project ID |
| `ER_SITE` | EarthRanger base URL (e.g. `https://your-site.pamdas.org`) |
| `EARTHRANGER_TOKEN` | EarthRanger bearer token |
| `BQ_DATASET_ID` | Target BigQuery dataset (default: `earthranger_data_v7`) |

## BigQuery schema

All tables use a consistent raw schema:

| Column | Type | Description |
|---|---|---|
| `id` | STRING | EarthRanger object ID |
| `data` | JSON | Full API response as JSON |
| `loaded_at` | TIMESTAMP | When the row was ingested |
| `event_timestamp` | TIMESTAMP | Object's own timestamp (for partitioning) |

Tables are time-partitioned. Query the `data` column with `JSON_VALUE()` / `JSON_QUERY()`.

## License

MIT
