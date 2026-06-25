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

print("=" * 60)
print("KAFKA PRODUCER - REAL TIME STOCK DATA")
print("=" * 60)
print("Mengirim data real-time ke Kafka topic...")
print("(Ctrl+C untuk berhenti)")
print("-" * 60)

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
                    print(f" {message['timestamp']} | {message['ticker']} | "
                        f"Open: {message['open']:.0f} | Close: {message['close']:.0f}")
            except Exception as e:
                print(f" Error: {e}")

        time.sleep(60) # kirim setiap 1 menit
except KeyboardInterrupt:
    print("\n Menghentikan producer...")
finally:
    producer.close()
