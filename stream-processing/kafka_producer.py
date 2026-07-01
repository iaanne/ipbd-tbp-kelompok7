import logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

from kafka import KafkaProducer
import json
import yfinance as yf
import time
import os
from datetime import datetime

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "stock-stream-topic"

logger.info("=" * 60)
logger.info("KAFKA PRODUCER - REAL TIME STOCK DATA")
logger.info("=" * 60)
logger.info("Mengirim data real-time ke Kafka topic...")
logger.info("(Ctrl+C untuk berhenti)")
logger.info("-" * 60)

SAHAM_REALTIME = ["BBCA.JK", "BBRI.JK", "TLKM.JK"]

try:
    while True:
        for ticker in SAHAM_REALTIME:
            try:
                # ambil data terbaru (real-time) dari yfinance
                stock = yf.Ticker(ticker)
                data = stock.history(period="1d", interval="1m").tail(1)

                if not data.empty:
                    message =  {
                        "timestamp" : datetime.now().isoformat(),
                        "ticker" : ticker.replace('.JK', ''),
                        "open" : float(data['Open'].values[0]),
                        "high" : float(data['High'].values[0]),
                        "low" : float(data['Low'].values[0]),
                        "close" : float(data['Close'].values[0]),
                        "volume" : int(data['Volume'].values[0])
                    }

                    # kirim ke Kafka Topic
                    producer.send(TOPIC, message)
                    logger.info(f" {message['timestamp']} | {message['ticker']} | "
                        f"Open: {message['open']:.0f} | Close: {message['close']:.0f}")
            except Exception as e:
                logger.info(f" Error: {e}")

        time.sleep(60) # kirim setiap 1 menit
except KeyboardInterrupt:
    logger.info("\n Menghentikan producer...")
finally:
    producer.close()
