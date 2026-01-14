# BoomboomBakudan: Real-Time Crypto MLOps Pipeline

A robust, real-time cryptocurrency price prediction pipeline. This project captures live data from Binance, processes it through a scalable architecture, and uses an LSTM model to predict future prices, all managed within an MLOps framework.

## 🚀 Key Features

- **Real-time Data Ingestion**: Captures live OHLCV data from Binance WebSocket.
- **Scalable Stream Processing**: Apache Spark handles stream processing and real-time inference.
- **MLOps Lifecycle**: 
    - **Experiment Tracking**: MLflow for logging parameters, metrics, and models.
    - **Automated Training**: LSTM model training pulling historical data from Cassandra.
    - **Scalable Serving**: FastAPI-based microservice for model inference.
- **High-Performance Storage**: Cassandra NoSQL database for time-series crypto data.
- **Visualization**: Grafana dashboards for monitoring prices and model performance.
- **Fully Containerized**: Deploy the entire stack using Docker Compose.

## 🏗️ Architecture

```
┌──────────────┐    ┌────────────┐    ┌─────────────────┐    ┌─────────────┐
│  Binance     │    │            │    │ Spark Streaming │    │             │
│  WebSocket   │───▶│  Redpanda  │───▶│ (Inference via  │───▶│  Cassandra  │
│  (Producer)  │    │  (Kafka)   │    │  FastAPI)       │    │  Database   │
└──────────────┘    └────────────┘    └────────┬────────┘    └──────┬──────┘
                                               │                    │
                                     ┌─────────┴─────────┐          ▼
                                     │  MLflow & Trainer │    ┌─────────────┐
                                     │ (LSTM Management) │    │   Grafana   │
                                     └───────────────────┘    └─────────────┘
```

## 🛠️ Stack

- **Languages**: Python, PySpark
- **Big Data**: Apache Spark, Redpanda (Kafka)
- **Database**: Apache Cassandra
- **ML/MLOps**: TensorFlow (LSTM), MLflow, FastAPI
- **Ops**: Docker, Docker Compose
- **Visuals**: Grafana

## 📋 Installation

### Prerequisites

- Docker & Docker Compose
- Git

### Setup

1.  **Clone & Navigate**:
    ```bash
    git clone https://github.com/yourusername/BoomboomBakudan.git
    cd BoomboomBakudan
    ```

2.  **Configure Environment**:
    ```bash
    cp .env.example .env
    # Edit .env if you need custom credentials
    ```

3.  **Launch the Factory**:
    ```bash
    docker-compose up -d
    ```

## 📈 MLOps Workflow

### 1. Training the Model
Use the trainer service to train the LSTM model on historical data stored in Cassandra:
```bash
docker-compose run binance-trainer
```

### 2. Experiment Tracking
Access the **MLflow UI** at [http://localhost:5000](http://localhost:5000) to:
- Compare training runs.
- View metrics (MAE, RMSE, MAPE, Hit Rate).
- Download or register model versions.

### 3. Real-Time Inference
The `binance-consumer` service automatically calls the `binance-serving` API (FastAPI) at port `8000` to get real-time price predictions while processing the stream.

## 📊 Monitoring

- **Redpanda Console**: [http://localhost:1003](http://localhost:1003) - Monitor data topics.
- **Grafana**: [http://localhost:3000](http://localhost:3000) - View live price and prediction charts.
- **Spark UI**: [http://localhost:4010](http://localhost:4010) - Monitor processing performance.

## 📝 Configuration (Environment Variables)

| Variable | Description |
|----------|-------------|
| `MLFLOW_TRACKING_URI` | URI for the MLflow server |
| `REDPANDA_BROKERS` | Address of Redpanda brokers |
| `ASSET_CASSANDRA_HOST` | Hostname for Cassandra |
| `ASSET_PRICES_TOPIC` | Topic name for streaming data |

---
*Created with ❤️ by the BoomboomBakudan Team.*
