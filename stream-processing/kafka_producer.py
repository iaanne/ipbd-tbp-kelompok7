from kafka import KafkaProducer
import json
impport yfinance as yf
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serialier=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "stock-stream-topic"

print("=" * 60)
print("KAFKA PRODUCER - REAL TIME STOCK DATA")
print("=" * 60)
print("Mengirim data real-time ke Kafka topic...")
print("(Ctrl+C untuk berhenti)")
print("-" * 60)

SAHAM_REALTIME = ["BBCA.JK", "BBRI.JK", "TKLM.JK"]

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
                    f"Open: {message['Open']:.0f} | Close: {message['close']:.0f}")
        except Exception as e:
            print(f" Error: {e}")

    time.sleep(60) # kirim setiap 1 menit

producer.close()
