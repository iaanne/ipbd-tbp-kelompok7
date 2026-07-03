import os, json, boto3, glob, sys, logging, requests
from datetime import datetime, timedelta
from botocore.config import Config
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ES_URL = os.getenv('ES_URL', 'http://localhost:9200')
GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://localhost:3900')
GARAGE_ACCESS_KEY = os.environ['GARAGE_ACCESS_KEY']
GARAGE_SECRET_KEY = os.environ['GARAGE_SECRET_KEY']
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')

SEVERITIES = ['INFO', 'DEBUG', 'WARNING', 'FATAL']
PIPELINES = ['batch', 'stream', 'ml-training', 'data-quality', 'pii-masking']

def get_garage_client():
    return boto3.client('s3',
        endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY,
        aws_secret_access_key=GARAGE_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='garage'
    )

def generate_sample_logs():
    logs = []
    base = datetime.now()
    for day_offset in range(7):
        ts = base - timedelta(days=day_offset)
        for hour in range(6, 23, 2):
            for pl in PIPELINES:
                sev = SEVERITIES[hash(f"{pl}-{day_offset}-{hour}") % 4]
                messages = {
                    'INFO': [f"{pl} started successfully", f"Processing {hash(str(day_offset+hour)) % 100 + 10} records",
                             f"Stage 1/{hash(str(day_offset+hour)) % 3 + 2} completed", "Connection established"],
                    'DEBUG': [f"Feature vector: {hash(str(day_offset+hour)) % 1000}", f"Cache hit ratio: {hash(str(day_offset+hour)) % 50 + 50}%",
                              f"Memory: {hash(str(day_offset+hour)) % 512 + 128}MB", f"Batch size: {hash(str(day_offset+hour)) % 100 + 50}"],
                    'WARNING': [f"High memory usage: {hash(str(day_offset+hour)) % 20 + 80}%",
                                f"Slow query detected ({hash(str(day_offset+hour)) % 5000 + 1000}ms)",
                                f"Retry attempt {hash(str(day_offset+hour)) % 3 + 1}/3",
                                f"API rate limit approaching"],
                    'FATAL': [f"Pipeline {pl} crashed: OOM error",
                              f"Connection timeout to Garage S3 after {hash(str(day_offset+hour)) % 30 + 10}s",
                              f"Data corruption detected in batch {hash(str(day_offset+hour)) % 100}",
                              f"Kafka consumer offset commit failed"],
                }
                msg = messages[sev][hash(f"{day_offset}-{hour}-{pl}") % len(messages[sev])]
                log_entry = {
                    '@timestamp': ts.replace(hour=hour, minute=hash(f"{day_offset}-{hour}-{pl}-minute") % 60).isoformat(),
                    'severity': sev,
                    'pipeline': pl,
                    'message': msg,
                    'host': 'ipbd-pipeline-node-01',
                    'environment': 'production',
                    'run_id': hash(f"{day_offset}-{hour}-{pl}") % 9999,
                    'duration_ms': hash(f"{day_offset}-{hour}-{pl}-duration") % 30000 + 100 if sev != 'FATAL' else None,
                }
                log_entry['duration_ms'] = log_entry.get('duration_ms') or 0
                if sev == 'FATAL':
                    log_entry['error_code'] = hash(msg) % 9999
                    log_entry['stack_trace'] = f"Traceback (most recent call last):\n  File \"/opt/pipeline/{pl}.py\", line {hash(msg) % 200}, in run\n    raise RuntimeError(\"{msg}\")\nRuntimeError: {msg}"
                logs.append(log_entry)

    logger.info(f"Generated {len(logs)} sample log entries")
    return logs

def read_garage_logs(client):
    logs = []
    prefixes = ['logs/', 'metadata/', 'audit/']
    for prefix in prefixes:
        try:
            objs = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            for o in objs.get('Contents', []):
                obj = client.get_object(Bucket=BUCKET, Key=o['Key'])
                content = obj['Body'].read().decode('utf-8', errors='ignore')
                try:
                    data = json.loads(content)
                    if isinstance(data, dict):
                        data['@timestamp'] = data.get('timestamp') or data.get('@timestamp') or datetime.now().isoformat()
                        data['severity'] = data.get('severity') or data.get('level') or 'INFO'
                        data['pipeline'] = data.get('pipeline') or data.get('flow') or 'unknown'
                        data['message'] = data.get('message') or data.get('status') or json.dumps(data)
                        data['source_file'] = o['Key']
                        logs.append(data)
                except json.JSONDecodeError:
                    logs.append({
                        '@timestamp': o['LastModified'].isoformat(),
                        'severity': 'INFO',
                        'pipeline': 'unknown',
                        'message': content[:500],
                        'source_file': o['Key'],
                    })
        except Exception:
            pass
    logger.info(f"Read {len(logs)} logs from Garage")
    return logs

def read_local_logs():
    logs = []
    for log_file in glob.glob('logs/**/*.log', recursive=True):
        try:
            with open(log_file) as f:
                content = f.read()
            logs.append({
                '@timestamp': datetime.fromtimestamp(os.path.getmtime(log_file)).isoformat(),
                'severity': 'INFO',
                'pipeline': 'batch' if 'batch' in log_file else 'stream' if 'stream' in log_file else 'ml' if 'ml' in log_file else 'unknown',
                'message': content[:500],
                'source_file': log_file,
            })
        except Exception:
            pass
    logger.info(f"Read {len(logs)} logs from local files")
    return logs

def push_to_elasticsearch(logs):
    try:
        r = requests.get(f"{ES_URL}/", timeout=5)
        r.raise_for_status()
    except Exception as e:
        raise Exception(f"Cannot connect to ES at {ES_URL}: {e}")
    logger.info(f"Connected to ES. Indexing {len(logs)} documents...")

    bulk_body = ""
    for i, log in enumerate(logs):
        action = json.dumps({"index": {"_index": "pipeline-logs", "_id": i}})
        doc = json.dumps({k: v for k, v in log.items() if v is not None}, default=str)
        bulk_body += action + "\n" + doc + "\n"

    r = requests.post(
        f"{ES_URL}/_bulk",
        data=bulk_body.encode('utf-8'),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=30,
    )
    result = r.json()
    success = result.get('items', [])
    logger.info(f"Indexed {len(success)} documents (errors: {result.get('errors', False)})")
    return len(success)

def setup_kibana():
    kibana_url = "http://localhost:5601"

    for attempt in range(30):
        try:
            r = requests.get(f"{kibana_url}/api/status", timeout=5)
            if r.status_code == 200:
                break
        except requests.exceptions.ConnectionError:
            pass
        if attempt < 29:
            import time
            time.sleep(5)

    requests.post(f"{kibana_url}/api/data_views/data_view",
        headers={
            'kbn-xsrf': 'true',
            'Content-Type': 'application/json',
        },
        json={
            'data_view': {
                'title': 'pipeline-logs*',
                'name': 'Pipeline Logs',
                'timeFieldName': '@timestamp',
            },
            'override': True,
        },
        timeout=10,
    )
    logger.info("Kibana index pattern created")

if __name__ == '__main__':
    logger.info("=== Push Logs to ELK ===")

    all_logs = generate_sample_logs()
    try:
        client = get_garage_client()
        all_logs += read_garage_logs(client)
    except Exception:
        pass
    all_logs += read_local_logs()

    push_to_elasticsearch(all_logs)
    setup_kibana()
    logger.info("Done! Open http://localhost:5601 → Pipeline Logs")
