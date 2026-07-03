from prefect import flow, task
import os, logging, boto3, json
from botocore.config import Config
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.environ['GARAGE_ACCESS_KEY']
GARAGE_SECRET_KEY = os.environ['GARAGE_SECRET_KEY']
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')

def get_garage_client():
    return boto3.client('s3', endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY, aws_secret_access_key=GARAGE_SECRET_KEY,
        config=Config(signature_version='s3v4'), region_name='garage')

@task
def check_kafka():
    from kafka import KafkaAdminClient
    try:
        admin = KafkaAdminClient(bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')])
        topics = admin.list_topics()
        ok = 'stock-stream-topic' in topics
        logger.info(f"Kafka topic: {'OK' if ok else 'MISSING'}")
        return "TOPIC_OK" if ok else "TOPIC_MISSING"
    except Exception as e:
        logger.error(f"Kafka failed: {e}")
        raise

@task
def check_garage():
    client = get_garage_client()
    buckets = [b['Name'] for b in client.list_buckets().get('Buckets', [])]
    logger.info(f"Garage buckets: {buckets}")
    return buckets

@task
def count_stream_data():
    client = get_garage_client()
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='stream-data/')
    count = len(objs.get('Contents', []))
    logger.info(f"Stream files: {count}")
    return count

@flow(log_prints=True)
def stream_monitor_flow():
    check_kafka()
    check_garage()
    count_stream_data()

if __name__ == "__main__":
    stream_monitor_flow()
