from prefect import flow, task
import os, json, logging
from datetime import datetime

logger = logging.getLogger(__name__)
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://influxdb:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', '')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'stock-org')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'pipeline-metrics')

@task
def push_log_metrics():
    import requests
    if not INFLUXDB_TOKEN:
        logger.warning("InfluxDB not configured")
        return

    lines = [
        f'pipeline_logs,severity=INFO,flow=batch_pipeline message="Batch pipeline completed" {int(datetime.now().timestamp())}000000000',
        f'pipeline_logs,severity=DEBUG,flow=feature_engineering message="Features extracted" {int(datetime.now().timestamp())}000000000',
        f'pipeline_logs,severity=WARNING,flow=data_quality message="Null values detected" {int(datetime.now().timestamp())}000000000',
        f'pipeline_logs,severity=INFO,flow=stream_monitor message="Kafka topic OK" {int(datetime.now().timestamp())}000000000',
        f'pipeline_metrics,batch=stock_batch rows_processed=1000,duration_seconds=120 {int(datetime.now().timestamp())}000000000',
    ]

    url = f"{INFLUXDB_URL}/api/v2/write?org={INFLUXDB_ORG}&bucket={INFLUXDB_BUCKET}&precision=s"
    headers = {'Authorization': f'Token {INFLUXDB_TOKEN}', 'Content-Type': 'text/plain'}
    for line in lines:
        resp = requests.post(url, headers=headers, data=line)
        if resp.status_code not in [204, 200]:
            logger.error(f"InfluxDB write failed: {resp.text}")

    logger.info("Metrics pushed to InfluxDB")

@flow(log_prints=True)
def push_metrics_flow():
    push_log_metrics()

if __name__ == "__main__":
    push_metrics_flow()
