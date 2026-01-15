# BoomboomBakudan: MLOps Pipeline Dự Đoán Giá Bitcoin Real-Time

Dự án xây dựng một pipeline MLOps hoàn chỉnh để dự đoán giá tiền điện tử (Bitcoin) theo thời gian thực. Hệ thống thu thập dữ liệu trực tiếp từ Binance, xử lý qua Kafka & Spark, lưu trữ tại Cassandra, và sử dụng mô hình LSTM để dự đoán giá tương lai.

## 🚀 Tính Năng Chính

- **Real-time Data**: Thu thập dữ liệu nến (OHLCV) từ Binance WebSocket.
- **Microservices**: Kiến trúc container hóa toàn bộ với Docker Compose.
- **Stream Processing**: Apache Spark xử lý dữ liệu streaming và gọi API dự đoán.
- **MLOps Lifecycle**: 
    - **MLflow**: Quản lý thí nghiệm, log metrics, và versioning mô hình.
    - **CI/CD**: Tự động test và deploy với GitHub Actions (Self-hosted Runner).
    - **Retraining**: Pipeline huấn luyện lạị mô hình khi có dữ liệu mới.
- **Monitoring**: Dashboard Grafana theo dõi giá và hiệu suất mô hình.

## 🏗️ Kiến Trúc

```
┌──────────────┐    ┌────────────┐    ┌─────────────────┐    ┌─────────────┐
│  Binance     │    │            │    │ Spark Streaming │    │             │
│  WebSocket   │───▶│  Redpanda  │───▶│ (Consumer &     │───▶│  Cassandra  │
│  (Producer)  │    │  (Kafka)   │    │  Inference)     │    │  (Database) │
└──────────────┘    └────────────┘    └────────┬────────┘    └──────┬──────┘
                                               │                    │
                                     ┌─────────┴─────────┐          ▼
                                     │  Serving API      │    ┌─────────────┐
                                     │  (FastAPI)        │    │   Grafana   │
                                     └─────────▲─────────┘    └─────────────┘
                                               │
                                     ┌─────────┴─────────┐
                                     │  MLflow & Trainer │
                                     │  (LSTM Model)     │
                                     └───────────────────┘
```

## �️ Cài Đặt & Chạy Dự Án

### 1. Yêu Cầu
- Docker & Docker Compose
- Git

### 2. Cấu Hình Biến Môi Trường (.env)
Dự án đã có file `.env` chuẩn. Nội dung đầy đủ như sau (lưu vào file `.env` tại thư mục gốc):

```properties
# --- Redpanda (Kafka) ---
REDPANDA_BROKERS=binance-redpanda:29092
ASSET_PRICES_TOPIC=data.asset_prices

# --- Cassandra (Database) ---
ASSET_CASSANDRA_HOST=binance-cassandra
ASSET_CASSANDRA_PORT=9042
ASSET_CASSANDRA_KEYSPACE=assets
ASSET_CASSANDRA_TABLE=assets
ASSET_CASSANDRA_USERNAME=adminadmin
ASSET_CASSANDRA_PASSWORD=adminadmin

# --- Spark ---
SPARK_MASTER_URL=spark://binance-consumer:7077
ASSET_SCHEMA_LOCATION=/src/schemas/assets.avsc

# --- MLOps ---
MLFLOW_TRACKING_URI=http://mlflow:5000
```

### 3. Khởi Chạy
Chạy lệnh sau để build và start toàn bộ hệ thống:

```bash
docker-compose up -d --build
```

### 4. Kiểm Tra Các Dịch Vụ
Sau khi khởi chạy thành công:
- **Redpanda Console**: [http://localhost:1003](http://localhost:1003) (Xem dữ liệu streaming)
- **Grafana**: [http://localhost:3000](http://localhost:3000) (User/Pass: `admin`/`admin`)
- **Spark UI**: [http://localhost:4010](http://localhost:4010) (Xem job xử lý)
- **MLflow UI**: [http://localhost:5000](http://localhost:5000) (Xem model training)
- **Serving API**: [http://localhost:8000/docs](http://localhost:8000/docs) (Test API)

## 🧪 Cách Test Hệ Thống

### Test Serving API
Mô hình yêu cầu input là 10 giá trị `close` price gần nhất.

**Cách 1: Dùng Curl**
```bash
curl -X POST "http://localhost:8000/predict" \
     -H "Content-Type: application/json" \
     -d '{"data": [42000, 42010, 42005, 42020, 42015, 42030, 42025, 42040, 42035, 42050]}'
```

**Cách 2: Dùng Python**
```python
import requests
data = [42000, 42010, 42005, 42020, 42015, 42030, 42025, 42040, 42035, 42050]
response = requests.post("http://localhost:8000/predict", json={"data": data})
print(response.json())
```

### Test Automatic Training
Để kích hoạt training thủ công (hoặc chờ CI/CD):
```bash
docker-compose run --rm binance-trainer
```

## 🔄 CI/CD (GitHub Actions)

Dự án hỗ trợ **Self-Hosted Runner**.
1. Đăng ký runner tại GitHub Repo > Settings > Actions > Runners.
2. Chạy runner trên máy của bạn (hoặc VPS).
3. Mỗi khi có commit mới vào `main`:
   - Hệ thống tự động chạy Unit Test (`models/test_lstm.py`).
   - Nếu pass, tự động chạy lệnh `docker-compose up -d --build` để cập nhật code mới nhất.

---
*Created by BoomboomBakudan Team*
