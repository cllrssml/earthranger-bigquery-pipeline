import os
import json
import logging
import argparse
import requests
import time
from datetime import datetime, timezone
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'your-gcp-project')
DATASET_ID = os.environ.get('BQ_DATASET_ID', 'earthranger_data_v7')
ER_TOKEN = os.environ.get('EARTHRANGER_TOKEN')
BASE_URL = os.environ.get('ER_SITE', 'https://your-site.pamdas.org') + '/api/v1.0'

# Performance Standards
PAGE_SIZE_OBS = 5000
PAGE_SIZE_EVENTS = 200
PAGE_SIZE_PATROLS = 100
PAGE_SIZE_SNAP = 2000

# --- SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger()
client = bigquery.Client(project=PROJECT_ID)

def get_headers():
    return {'Authorization': f"Bearer {ER_TOKEN}", 'Accept': 'application/json'}

def get_session():
    """Configured for 3 retries with an exponential backoff factor."""
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def standardise_time(ts_str):
    """Converts EarthRanger time strings into pure UTC datetime objects."""
    if not ts_str:
        return None
    try:
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1] + '+00:00'
        dt = datetime.fromisoformat(ts_str)
        return dt.astimezone(timezone.utc).replace(microsecond=0)
    except ValueError as e:
        logger.error(f"Time parsing error for {ts_str}: {e}")
        return None

# --- STATE MANAGEMENT ---
def get_cursor(key, default_start="2019-01-01T00:00:00+00:00"):
    query = f"SELECT cursor_value FROM `{PROJECT_ID}.{DATASET_ID}.elt_state` WHERE pipeline_key_pk = '{key}'"
    try:
        rows = list(client.query(query).result())
        if rows: return rows[0].cursor_value
    except Exception: pass
    return default_start

def save_cursor(key, value):
    query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.elt_state` T
        USING (SELECT '{key}' as k, '{value}' as v, CURRENT_TIMESTAMP() as ts) S
        ON T.pipeline_key_pk = S.k
        WHEN MATCHED THEN UPDATE SET cursor_value = S.v, last_updated_ts = S.ts
        WHEN NOT MATCHED THEN INSERT (pipeline_key_pk, cursor_value, last_updated_ts) VALUES (k, v, ts)
    """
    try: client.query(query).result()
    except Exception as e: logger.error(f"⚠️ Failed to save cursor: {e}")

# --- BIGQUERY UTILS (STREAMING VERSION) ---
def ensure_table(table_name, is_snapshot=False):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    if is_snapshot:
        try:
            client.get_table(table_id)
            client.query(f"TRUNCATE TABLE `{table_id}`").result()
            logger.info(f"🧹 Truncated {table_name}")
            return
        except: pass 

    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED")
    ]
    if not is_snapshot:
        schema.append(bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"))

    try:
        table = bigquery.Table(table_id, schema=schema)
        if is_snapshot: table.time_partitioning = bigquery.TimePartitioning(field="loaded_at")
        else: table.time_partitioning = bigquery.TimePartitioning(field="event_timestamp")
        client.create_table(table, exists_ok=True)
    except Exception as e:
        if "Already Exists" not in str(e): logger.warning(f"Table check issue: {e}")

def extract_time(record, table):
    if 'patrols' in table:
        try: return record['patrol_segments'][0]['time_range']['start_time']
        except: return record.get('created_at')
    if 'events' in table: return record.get('time') or record.get('updated_at')
    if 'observations' in table: return record.get('recorded_at')
    return None

def load_batch(rows, table, is_snapshot):
    """STREAMING INSERT METHOD"""
    if not rows: return
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    bq_rows = []
    now = datetime.now(timezone.utc).isoformat()
    
    for r in rows:
        row_data = {"id": str(r.get('id', 'unknown')), "data": json.dumps(r), "loaded_at": now}
        if not is_snapshot:
            row_data["event_timestamp"] = extract_time(r, table) or now
        bq_rows.append(row_data)
    
    errors = client.insert_rows_json(table_id, bq_rows)
    if errors: logger.error(f"❌ BQ Insert Error: {errors[:1]}")

# --- ENGINES ---
def run_snapshot_task():
    targets = {
        "assignments": "/subjectsources/", "subjects": "/subjects/", "users": "/users/",
        "event_types": "/activity/events/eventtypes/", "sources": "/sources/", "choices": "/choices/"
    }
    logger.info("📸 STARTING SNAPSHOT_ALL")
    session = get_session()
    for key, endpoint in targets.items():
        table_name = f"raw_er_{key}"
        ensure_table(table_name, is_snapshot=True)
        url = f"{BASE_URL}{endpoint}?page_size={PAGE_SIZE_SNAP}"
        count = 0
        while url:
            try:
                r = session.get(url, headers=get_headers(), timeout=300)
                r.raise_for_status()
                data = r.json().get('data', {})
                results = data if isinstance(data, list) else data.get('results', [])
                url = None if isinstance(data, list) else data.get('next')
                if results:
                    load_batch(results, table_name, is_snapshot=True)
                    count += len(results)
            except Exception as e:
                logger.error(f"Snapshot Error {key}: {e}"); break
        logger.info(f"   ✅ {key.upper()}: {count} rows")

def run_resumable_task(target):
    if target == 'observations':
        url_base = "/observations/"; table = "raw_er_observations"; page_size = PAGE_SIZE_OBS
    elif target == 'events':
        url_base = "/activity/events/"; table = "raw_er_events"; page_size = PAGE_SIZE_EVENTS
    elif target == 'patrols':
        url_base = "/activity/patrols/"; table = "raw_er_patrols"; page_size = PAGE_SIZE_PATROLS
    
    ensure_table(table, is_snapshot=False)
    cursor_key = f"v7_cursor_{target}"
    start_dt_str = get_cursor(cursor_key)
    logger.info(f"🚀 STARTING {target.upper()} | RESUMING FROM: {start_dt_str}")

    params = {"page_size": page_size}
    if target == "patrols": 
        params['filter'] = json.dumps({"date_range": {"lower": start_dt_str}})
    elif target == "events":
        params['updated_since'] = start_dt_str; params['include_details'] = 'true'; params['sort_by'] = 'updated_at'
    else: # Observations
        params['created_after'] = start_dt_str; params['sort_by'] = 'created_at'; params['use_cursor'] = 'true' 

    session = get_session(); url = f"{BASE_URL}{url_base}"; total_session = 0
    max_cursor_dt = standardise_time(start_dt_str)
    
    while url:
        try:
            r = session.get(url, headers=get_headers(), params=params, timeout=300)
            r.raise_for_status()
            data = r.json().get('data', {})
            results = data.get('results', [])
            if not results:
                logger.info("   ✅ Reached end of stream."); break
            
            load_batch(results, table, is_snapshot=False)
            total_session += len(results)
            
            for item in results:
                raw_ts = None
                if target == 'patrols':
                    try: raw_ts = item['patrol_segments'][0]['time_range']['start_time']
                    except: raw_ts = item.get('created_at')
                elif target == 'events': raw_ts = item.get('updated_at')
                elif target == 'observations': raw_ts = item.get('created_at') or item.get('recorded_at')
                
                clean_ts = standardise_time(raw_ts)
                if clean_ts and max_cursor_dt and clean_ts > max_cursor_dt: 
                    max_cursor_dt = clean_ts
            
            save_cursor(cursor_key, max_cursor_dt.isoformat())
            if total_session % (page_size * 5) == 0: 
                logger.info(f"   ⏱️ {total_session} rows. Cursor: {max_cursor_dt.isoformat()}")
            
            url = data.get('next'); params = {} 
        except Exception as e:
            logger.error(f"🛑 Stream Error (Circuit Breaker Triggered): {e}")
            break
            
    logger.info(f"🏁 SESSION COMPLETE: {total_session} rows added.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', required=True, choices=['observations', 'events', 'patrols', 'snapshot_all'])
    args = parser.parse_args()
    if args.task == 'snapshot_all': run_snapshot_task()
    else: run_resumable_task(args.task)