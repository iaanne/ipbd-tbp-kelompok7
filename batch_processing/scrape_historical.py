# data historis dari yfinance 
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

SAHAM_LIST = {
    "^JKSE": "IDX_Composite",
    "BBCA.JK": "Bank_BCA",
    "BBRI.JK": "Bank_BRI",
    "BMRI.JK": "Bank_Mandiri",
    "TLKM.JK": "Telkom",
    "ASII.JK": "Astra",
}

# periode: 1 tahun ke belakang (data historis)
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=365)

print("=" * 60)
print("SCRAPING DATA HISTORIS SAHAM INDONESIA")
print("=" * 60)
print(f"Periode: {START_DATE.date()} s/d {END_DATE.date()}")
print("-" * 60)

all_data = []

for ticker, nama in SAHAM_LIST.items():
    print(f"\n Download {ticker} ({nama})...")
    
    # download dari Yahoo Finance
    stock = yf.Ticker(ticker)
    df = stock.history(start=START_DATE, end=END_DATE)
    
    if df.empty:
        print(f"  Gagal download {ticker}")
        continue
    
    # format data
    df = df.reset_index()
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df['Ticker'] = ticker.replace('.JK', '').replace('^', '')
    df['Nama_Saham'] = nama
    
    # FOKUS: Open dan Close (plus High, Low, Volume untuk ML)
    df_clean = df[[
        'Date', 'Ticker', 'Nama_Saham',
        'Open', 'High', 'Low', 'Close', 'Volume'
    ]].copy()
    
    # hitung perubahan harian
    df_clean['Daily_Change'] = df_clean['Close'] - df_clean['Open']
    df_clean['Daily_Change_Pct'] = (df_clean['Daily_Change'] / df_clean['Open']) * 100
    
    all_data.append(df_clean)
    print(f" {len(df_clean)} baris data")

# gabungkan semua saham
df_final = pd.concat(all_data, ignore_index=True)

# simpan ke CSV 
output_file = "data/saham_indonesia_historical.csv"
os.makedirs("data", exist_ok=True)
df_final.to_csv(output_file, index=False)

print("\n" + "=" * 60)
print(" SCRAPING SELESAI!")
print("=" * 60)
print(f" File CSV tersimpan: {output_file}")
print(f" Total baris: {len(df_final)}")
print(f" Total saham: {df_final['Ticker'].nunique()}")
print(f"\n Preview data:")
print(df_final[['Date', 'Ticker', 'Open', 'Close', 'Daily_Change']].head(10).to_string())
