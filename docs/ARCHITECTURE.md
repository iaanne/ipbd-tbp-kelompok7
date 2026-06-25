# Arsitektur Pipeline - Real-time Ticker & Prediksi Saham

## Diagram Arsitektur

```mermaid
graph TB
    subgraph "Data Source"
        YF[YFinance API]
        IDX[IDX Market Data]
    end

    subgraph "Ingestion Layer"
        SP[scrape_historical.py<br/>Batch Scraper]
        KP[kafka_producer.py<br/>Stream Producer]
    end

    subgraph "Message Broker"
        K[Kafka<br/>stock-stream-topic]
    end

    subgraph "Storage Layer"
        G[Garage S3<br/>Object Storage]
        subgraph "Garage Buckets"
            RAW[raw-data/]
            PROC[processed-data/]
            FEAT[features/]
            PRED[predictions/]
            MODEL[models/]
            META[metadata/]
            AUDIT[audit/]
            PII[pii-sample/]
        end
    end

    subgraph "Processing Layer"
        SB[spark_batch_etl.py<br/>Spark Batch ETL]
        SS[spark_streaming.py<br/>Spark Streaming]
        FE[feature_engineering.py<br/>Feature Engineering]
        KM[train_model.py<br/>KMeans Clustering]
        BI[batch_inference.py<br/>Batch Prediction]
        RP[realtime_prediction.py<br/>Real-time Prediction]
        LSTM[lstm_train.py<br/>LSTM Time Series]
        LPRED[lstm_predict.py<br/>LSTM Prediction]
    end

    subgraph "Query Layer"
        T[Trino<br/>SQL Query Engine]
        HMS[Hive Metastore]
        PG_HMS[PostgreSQL<br/>HMS Metadata]
    end

    subgraph "ML Lifecycle"
        MLF[MLflow<br/>Experiment Tracking]
        MLF_REG[Model Registry]
    end

    subgraph "Orchestration"
        PS[Prefect Server<br/>Flow Orchestration]
        PA[Prefect Agent<br/>Flow Executor]
    end

    subgraph "Monitoring & Observability"
        P[Prometheus<br/>Metrics]
        AM[Alertmanager<br/>Notifications]
        I[InfluxDB<br/>Time-series]
        GRAF[Grafana<br/>Dashboards]
    end

    subgraph "Notification Channels"
        EMAIL[Email]
        TG[Telegram]
    end

    %% Data Flow
    YF --> SP
    IDX --> SP
    YF --> KP

    SP -->|CSV Upload| RAW
    KP -->|JSON| K
    K -->|Read| SS

    RAW --> SB
    SB -->|Parquet| PROC

    RAW --> FE
    PROC --> FE
    FE -->|Parquet| FEAT

    FEAT --> KM
    FEAT --> LSTM
    KM -->|model.joblib| MODEL
    LSTM -->|model.h5| MODEL
    KM -.->|track| MLF
    LSTM -.->|track| MLF

    FEAT --> BI
    MODEL --> BI
    BI -->|enriched| PRED

    K --> RP
    MODEL --> RP
    RP -->|predictions| PRED

    FEAT --> LPRED
    MODEL --> LPRED
    LPRED -->|predictions| PRED

    HMS --> PG_HMS
    G --> HMS
    T --> HMS
    T -->|SQL| G

    PS -->|schedule| PA
    PA -->|trigger| SP
    PA -->|trigger| SB
    PA -->|trigger| FE
    PA -->|trigger| KM
    PA -->|trigger| LSTM
    PA -->|trigger| BI

    P -->|scrape| GRAF
    P --> AM
    AM --> EMAIL
    AM --> TG

    I --> GRAF
    GRAF --> T

    %% Styles
    classDef source fill:#e1f5fe,stroke:#01579b
    classDef ingest fill:#f3e5f5,stroke:#7b1fa2
    classDef storage fill:#e8f5e9,stroke:#2e7d32
    classDef process fill:#fff3e0,stroke:#e65100
    classDef query fill:#fce4ec,stroke:#c62828
    classDef ml fill:#e8eaf6,stroke:#283593
    classDef orc fill:#f3e5f5,stroke:#4a148c
    classDef mon fill:#e0f2f1,stroke:#00695c

    class YF,IDX source
    class SP,KP ingest
    class G,RAW,PROC,FEAT,PRED,MODEL,META,AUDIT,PII storage
    class SB,SS,FE,KM,BI,RP,LSTM,LPRED process
    class T,HMS,PG_HMS query
    class MLF,MLF_REG ml
    class PS,PA orc
    class P,AM,I,GRAF mon
    class EMAIL,TG notify
```

## Alur Data

### 1. Batch Pipeline
```
[YFinance] --scrape--> [CSV] --upload--> [Garage: raw-data/]
    --> [Spark ETL] --transform--> [Garage: processed-data/]
```

### 2. Stream Pipeline
```
[YFinance] --kafka-producer--> [Kafka: stock-stream-topic]
    --> [Spark Streaming] --aggregate--> [Console / Garage]
```

### 3. ML Pipeline (KMeans)
```
[Garage: raw-data/] --feature-engineering--> [Garage: features/]
    --KMeans training--> [Garage: models/kmeans_*.joblib]
    --batch-inference--> [Garage: predictions/ (enriched)]
```

### 4. ML Pipeline (LSTM)
```
[Garage: raw-data/] --feature-engineering--> [Garage: features/]
    --LSTM training--> [Garage: models/lstm_ihsg_*.h5]
    --LSTM predict--> [Garage: predictions/lstm/]
```

### 5. ML Lifecycle (MLflow)
```
[Training] --log params/metrics--> [MLflow Tracking]
[Best model] --register--> [MLflow Model Registry]
[Model] --artifact--> [Garage: mlflow/]
```

### 6. Orchestration (Prefect)
```
[Prefect Server] --schedule--> [Prefect Agent]
    --batch_flow (18:00 setiap hari kerja)
    --ml_flow (06:00 Sen/Rab/Jum)
    --lstm_flow (06:00 Minggu)
    --dq_flow (07:00 setiap hari kerja)
    --stream_monitor (setiap 15 menit)
```

## Komponen Infrastruktur

| Service | Port | Fungsi |
|---------|------|--------|
| **Garage S3** | 3900 (S3 API), 3903 (Admin) | Object storage untuk data lake |
| **Garage WebUI** | 3909 | Admin web Garage |
| **Kafka** | 9092, 29092 | Message broker untuk stream |
| **Zookeeper** | 2181 | Koordinator Kafka |
| **Spark Master** | 8080 (UI), 7077 | Distributed processing |
| **Spark Worker** | - | Worker Spark |
| **Trino** | 8082 | Distributed SQL query engine |
| **Hive Metastore** | 9083 | Metadata management untuk Trino |
| **PostgreSQL (HMS)** | 5432 | Database metadata Hive |
| **MLflow** | 5000 | ML lifecycle tracking |
| **Prefect Server** | 4200 | Workflow orchestration |
| **Prefect Agent** | - | Executor flow |
| **Grafana** | 3000 | Dashboard visualisasi |
| **InfluxDB** | 8086 | Time-series database |
| **Prometheus** | 9090 | Metrics collection |
| **Alertmanager** | 9093 | Alert notification |

## Keamanan

- **Password disimpan di environment variable** — semua credentials via `.env`, tidak hardcoded
- **Masking PII**: Dataset fiktif dibuat untuk demo masking di `pii-sample/`
- **Garage credentials** dikonfigurasi via env, tidak di-commit ke git
- **Audit Trail**: Semua operasi pipeline tercatat di `audit/` di Garage
