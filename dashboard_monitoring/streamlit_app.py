import os
import json
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from botocore.config import Config
import streamlit as st

st.set_page_config(
    page_title="IPBD Stock Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "GKc98624849db70446555a905b")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828")
BUCKET = os.getenv("GARAGE_BUCKET", "stock-bucket")


@st.cache_resource
def get_garage_client():
    return boto3.client(
        "s3",
        endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY,
        aws_secret_access_key=GARAGE_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="garage",
    )


@st.cache_data(ttl=300)
def load_csv_from_garage(prefix, limit=5):
    client = get_garage_client()
    try:
        objs = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        files = sorted(
            objs.get("Contents", []),
            key=lambda x: x["LastModified"],
            reverse=True,
        )[:limit]
        dfs = []
        for f in files:
            if f["Key"].endswith(".csv"):
                obj = client.get_object(Bucket=BUCKET, Key=f["Key"])
                dfs.append(pd.read_csv(StringIO(obj["Body"].read().decode("utf-8"))))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_json_from_garage(key):
    client = get_garage_client()
    try:
        obj = client.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


@st.cache_data(ttl=300)
def list_files_in_prefix(prefix):
    client = get_garage_client()
    try:
        objs = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        return [
            {"key": o["Key"], "size": o["Size"], "modified": o["LastModified"]}
            for o in objs.get("Contents", [])
        ]
    except Exception:
        return []


st.sidebar.title("📊 IPBD Dashboard")
st.sidebar.markdown("**Stock Pipeline Monitoring**")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigasi",
    [
        "📈 IHSG Overview",
        "📊 Stock Analysis",
        "🤖 ML Predictions",
        "📋 Data Quality",
        "🔐 PII Masking",
        "📦 File Inventory",
    ],
)

st.sidebar.markdown("---")
st.sidebar.caption(f"Bucket: `{BUCKET}`")
st.sidebar.caption(f"Garage: `{GARAGE_ENDPOINT}`")

# ─────────────────────────────────────────────
# PAGE 1: IHSG OVERVIEW
# ─────────────────────────────────────────────
if page == "📈 IHSG Overview":
    st.title("📈 IHSG Overview")
    st.markdown("Analisa pergerakan IHSG dan indikator teknikal.")

    df = load_csv_from_garage("raw-data/")
    if df.empty:
        st.warning(
            "Belum ada data. Jalankan pipeline batch dulu: `bash scripts/run_batch.sh`"
        )
        st.stop()

    df["Date"] = pd.to_datetime(df["Date"])
    ihsg = df[df["Ticker"] == "^JKSE"].sort_values("Date").copy()
    if ihsg.empty:
        ihsg = df[df["Ticker"] == "JKSE"].sort_values("Date").copy()

    col1, col2, col3, col4 = st.columns(4)
    if not ihsg.empty:
        latest = ihsg.iloc[-1]
        prev = ihsg.iloc[-2] if len(ihsg) > 1 else latest
        change = latest["Close"] - prev["Close"]
        change_pct = (change / prev["Close"]) * 100

        col1.metric("Harga Close", f"{latest['Close']:.0f}", f"{change:+.0f}")
        col2.metric("Open", f"{latest['Open']:.0f}")
        col3.metric("Volume", f"{latest['Volume']:,.0f}")
        col4.metric(
            "Daily Change %", f"{latest.get('Daily_Change_Pct', 0):+.2f}%"
        )

        ihsg["SMA_7"] = ihsg["Close"].rolling(7).mean()
        ihsg["SMA_30"] = ihsg["Close"].rolling(30).mean()

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=ihsg["Date"],
                y=ihsg["Close"],
                mode="lines+markers",
                name="Close",
                line=dict(color="#00BFFF"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=ihsg["Date"],
                y=ihsg["SMA_7"],
                name="SMA 7",
                line=dict(color="#FFA500", dash="dash"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=ihsg["Date"],
                y=ihsg["SMA_30"],
                name="SMA 30",
                line=dict(color="#FF4500", dash="dash"),
            )
        )
        fig.update_layout(
            title="IHSG Close Price dengan SMA 7 & 30",
            xaxis_title="Date",
            yaxis_title="Price",
            height=500,
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

        ihsg["Returns_1d"] = ihsg["Close"].pct_change() * 100
        ihsg["Volatility_5d"] = ihsg["Returns_1d"].rolling(5).std()

        fig2 = go.Figure()
        fig2.add_trace(
            go.Bar(
                x=ihsg["Date"],
                y=ihsg["Volume"],
                name="Volume",
                marker_color="#4682B4",
            )
        )
        fig2.update_layout(
            title="Volume Perdagangan IHSG",
            xaxis_title="Date",
            yaxis_title="Volume",
            height=350,
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Data IHSG belum tersedia di dataset.")

# ─────────────────────────────────────────────
# PAGE 2: STOCK ANALYSIS
# ─────────────────────────────────────────────
elif page == "📊 Stock Analysis":
    st.title("📊 Stock Analysis")
    st.markdown("Perbandingan harga dan performa semua saham.")

    df = load_csv_from_garage("raw-data/")
    if df.empty:
        st.warning("Belum ada data. Jalankan pipeline batch dulu.")
        st.stop()

    df["Date"] = pd.to_datetime(df["Date"])

    tickers = df["Ticker"].unique()
    selected = st.multiselect("Pilih saham", tickers, default=list(tickers))

    if selected:
        filtered = df[df["Ticker"].isin(selected)]

        fig = px.line(
            filtered,
            x="Date",
            y="Close",
            color="Nama_Saham",
            title="Harga Close Semua Saham",
            markers=True,
        )
        fig.update_layout(height=500, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.bar(
            filtered,
            x="Date",
            y="Volume",
            color="Nama_Saham",
            title="Volume Perdagangan",
            barmode="group",
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

        latest_prices = (
            filtered.sort_values("Date")
            .groupby("Ticker")
            .last()
            .reset_index()
        )
        latest_prices["Change"] = latest_prices["Close"] - latest_prices["Open"]
        latest_prices["Change_Pct"] = (
            latest_prices["Change"] / latest_prices["Open"] * 100
        )

        cols = st.columns(len(selected))
        for i, row in latest_prices.iterrows():
            idx = list(latest_prices.index).index(i)
            cols[idx].metric(
                f"{row['Nama_Saham']}",
                f"{row['Close']:.0f}",
                f"{row['Change_Pct']:+.2f}%",
            )

        st.subheader("Data Mentah")
        st.dataframe(
            filtered.sort_values(["Ticker", "Date"]),
            use_container_width=True,
            hide_index=True,
        )

# ─────────────────────────────────────────────
# PAGE 3: ML PREDICTIONS
# ─────────────────────────────────────────────
elif page == "🤖 ML Predictions":
    st.title("🤖 Machine Learning Predictions")
    st.markdown("Hasil clustering KMeans, prediksi LSTM, dan estimasi IHSG 6000.")

    tab1, tab2, tab3 = st.tabs(["KMeans Clustering", "LSTM Prediction", "IHSG 6000"])

    with tab1:
        st.subheader("KMeans Clustering - Distribusi Cluster")

        try:
            predictions = load_csv_from_garage("predictions/")
            if not predictions.empty and "Cluster" in predictions.columns:
                cluster_counts = predictions["Cluster"].value_counts().reset_index()
                cluster_counts.columns = ["Cluster", "Count"]

                fig = px.pie(
                    cluster_counts,
                    values="Count",
                    names="Cluster",
                    title="Distribusi Cluster Saham",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig, use_container_width=True)

                fig2 = px.scatter(
                    predictions,
                    x="Returns_1d",
                    y="Volatility_5d",
                    color="Cluster",
                    hover_data=["Ticker", "Nama_Saham"],
                    title="Scatter Plot: Return vs Volatility (per Cluster)",
                    color_continuous_scale="Viridis",
                )
                fig2.update_layout(height=500)
                st.plotly_chart(fig2, use_container_width=True)

                st.dataframe(predictions, use_container_width=True, hide_index=True)
            else:
                st.info("Belum ada hasil clustering. Jalankan ML pipeline dulu.")
        except Exception:
            st.info("Belum ada data clustering. Jalankan: `bash scripts/run_ml.sh`")

    with tab2:
        st.subheader("LSTM Time Series Prediction")
        lstm_files = list_files_in_prefix("predictions/lstm/")
        if lstm_files:
            latest_lstm = lstm_files[0]["key"]
            data = load_json_from_garage(latest_lstm)
            if data:
                st.json(data)

                if "predictions" in data:
                    pred_df = pd.DataFrame(
                        list(data["predictions"].items()),
                        columns=["Date", "Predicted_Close"],
                    )
                    pred_df["Date"] = pd.to_datetime(pred_df["Date"])

                    fig = px.line(
                        pred_df,
                        x="Date",
                        y="Predicted_Close",
                        title="LSTM Prediction - 7 Hari ke Depan",
                        markers=True,
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)

                    st.metric(
                        "Latest Close",
                        f"{data.get('latest_close', 0):.0f}",
                    )
                    st.metric(
                        "Estimasi Hari ke 6000",
                        str(data.get("days_to_6000", "N/A")),
                    )
        else:
            st.info(
                "Belum ada prediksi LSTM. Jalankan: `python ml_integration/lstm_predict.py`"
            )

    with tab3:
        st.subheader("🎯 IHSG Target: 6000")
        st.markdown(
            """
        **Tujuan Bisnis:** Memprediksi kapan IHSG benar-benar menyentuh titik 6000
        (minimal 2 hari berturut-turut) dan melihat potensi market yang masih sehat untuk investasi.
        """
        )

        df = load_csv_from_garage("raw-data/")
        if not df.empty:
            ihsg = df[df["Ticker"] == "^JKSE"].sort_values("Date").copy()
            if ihsg.empty:
                ihsg = df[df["Ticker"] == "JKSE"].sort_values("Date").copy()

            if not ihsg.empty:
                latest_close = ihsg["Close"].iloc[-1]
                gap = 6000 - latest_close
                avg_change = ihsg["Close"].pct_change().mean()
                days_est = (
                    max(1, int(gap / (latest_close * avg_change)))
                    if avg_change > 0 and gap > 0
                    else "> 365 (tren negatif)"
                )

                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=ihsg["Date"],
                        y=ihsg["Close"],
                        mode="lines",
                        name="IHSG Close",
                        line=dict(color="#00BFFF"),
                    )
                )
                fig.add_hline(
                    y=6000,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Target 6000",
                )
                fig.update_layout(
                    title="IHSG Close Price menuju 6000",
                    xaxis_title="Date",
                    yaxis_title="Price",
                    height=500,
                )
                st.plotly_chart(fig, use_container_width=True)

                col1, col2, col3 = st.columns(3)
                col1.metric("IHSG Saat Ini", f"{latest_close:.0f}")
                col2.metric("Gap ke 6000", f"{gap:+.0f}")
                col3.metric("Estimasi Hari", str(days_est))

                ihsg["Above_6000"] = ihsg["Close"] >= 6000
                consecutive = (
                    ihsg["Above_6000"]
                    .groupby((~ihsg["Above_6000"]).cumsum())
                    .cumsum()
                )
                max_consecutive = consecutive.max()
                st.info(
                    f"Konfirmasi IHSG di atas 6000: **{max_consecutive} hari berturut-turut**"
                )

# ─────────────────────────────────────────────
# PAGE 4: DATA QUALITY
# ─────────────────────────────────────────────
elif page == "📋 Data Quality":
    st.title("📋 Data Quality & Governance")
    st.markdown("Laporan kualitas data, metadata, dan audit trail.")

    tab1, tab2, tab3 = st.tabs(["Quality Report", "Metadata", "Audit Trail"])

    with tab1:
        st.subheader("Data Quality Report")
        dq_files = list_files_in_prefix("metadata/data_quality")
        if dq_files:
            latest_dq = dq_files[0]["key"]
            report = load_json_from_garage(latest_dq)
            if report:
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Rows", report.get("total_rows", "N/A"))
                col2.metric("Total Columns", report.get("total_columns", "N/A"))
                col3.metric(
                    "Rows with Null",
                    f"{report.get('overall', {}).get('rows_with_null', 'N/A')}",
                )

                if "columns" in report:
                    cols_data = []
                    for col_name, col_info in report["columns"].items():
                        row = {"Column": col_name}
                        row.update(col_info)
                        cols_data.append(row)
                    cols_df = pd.DataFrame(cols_data)
                    st.dataframe(cols_df, use_container_width=True, hide_index=True)

                st.json(report)
        else:
            st.info(
                "Belum ada laporan kualitas data. Jalankan: `python dashboard_monitoring/data_quality.py`"
            )

        if st.button("Refresh Data Quality"):
            st.cache_data.clear()
            st.rerun()

    with tab2:
        st.subheader("Table Metadata")
        metadata = load_json_from_garage("metadata/table_metadata.json")
        if metadata:
            st.json(metadata)

            if "schema" in metadata:
                schema_df = pd.DataFrame(metadata["schema"])
                st.dataframe(schema_df, use_container_width=True, hide_index=True)
        else:
            st.info("Metadata belum tersedia.")

    with tab3:
        st.subheader("Audit Trail")
        audit_files = list_files_in_prefix("audit/")
        if audit_files:
            audits = []
            for f in audit_files:
                data = load_json_from_garage(f["key"])
                if data:
                    audits.append(data)
            if audits:
                audit_df = pd.DataFrame(audits)
                st.dataframe(audit_df, use_container_width=True, hide_index=True)
        else:
            st.info("Belum ada audit trail.")

# ─────────────────────────────────────────────
# PAGE 5: PII MASKING
# ─────────────────────────────────────────────
elif page == "🔐 PII Masking":
    st.title("🔐 PII Masking Demo")
    st.markdown("Demonstrasi masking data pribadi (PII) pada data trader.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Data Original")
        try:
            obj = get_garage_client().get_object(
                Bucket=BUCKET, Key="pii-sample/data_trader_original.csv"
            )
            orig_df = pd.read_csv(
                StringIO(obj["Body"].read().decode("utf-8"))
            )
            st.dataframe(orig_df, use_container_width=True, hide_index=True)
        except Exception:
            st.info(
                "Belum ada data PII. Jalankan: `python dashboard_monitoring/masking_pii.py`"
            )

    with col2:
        st.subheader("Data Masked")
        try:
            obj = get_garage_client().get_object(
                Bucket=BUCKET, Key="pii-sample/data_trader_masked.csv"
            )
            masked_df = pd.read_csv(
                StringIO(obj["Body"].read().decode("utf-8"))
            )
            st.dataframe(masked_df, use_container_width=True, hide_index=True)
        except Exception:
            st.info("Data masked belum tersedia.")

    docs = load_json_from_garage("pii-sample/masking_documentation.json")
    if docs:
        st.subheader("Dokumentasi Masking")
        st.json(docs)

# ─────────────────────────────────────────────
# PAGE 6: FILE INVENTORY
# ─────────────────────────────────────────────
elif page == "📦 File Inventory":
    st.title("📦 Garage S3 - File Inventory")
    st.markdown("Semua file yang tersimpan di Garage S3.")

    prefixes = [
        "raw-data/",
        "processed-data/",
        "features/",
        "models/",
        "predictions/",
        "metadata/",
        "audit/",
        "logs/",
        "pii-sample/",
    ]

    for prefix in prefixes:
        files = list_files_in_prefix(prefix)
        with st.expander(f"{prefix} ({len(files)} files)"):
            if files:
                file_df = pd.DataFrame(files)
                file_df["size_kb"] = file_df["size"] / 1024
                file_df["size_kb"] = file_df["size_kb"].round(2)
                file_df["modified"] = pd.to_datetime(file_df["modified"])
                st.dataframe(
                    file_df[["key", "size_kb", "modified"]],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.caption("(kosong)")

    st.sidebar.markdown("---")
    st.sidebar.info(
        "💡 **Tips:**\n"
        "1. Jalankan `bash scripts/run_pipeline.sh` untuk full pipeline\n"
        "2. Data otomatis terisi di halaman ini\n"
        "3. Refresh browser setelah pipeline selesai"
    )
