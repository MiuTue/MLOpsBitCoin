import os
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import mlflow
import mlflow.tensorflow
from sklearn.metrics import mean_absolute_error, mean_squared_error
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LSTMTrainer:
    def __init__(self):
        self.cassandra_host = os.environ.get('ASSET_CASSANDRA_HOST', 'localhost')
        self.cassandra_port = int(os.environ.get('ASSET_CASSANDRA_PORT', 9042))
        self.keyspace = os.environ.get('ASSET_CASSANDRA_KEYSPACE', 'assets')
        self.table = os.environ.get('ASSET_CASSANDRA_TABLE', 'assets')
        self.mlflow_uri = os.environ.get('MLFLOW_TRACKING_URI', 'http://localhost:5000')
        
        mlflow.set_tracking_uri(self.mlflow_uri)
        mlflow.set_experiment("Crypto_LSTM_Prediction")

    def fetch_data(self, asset_name='bitcoin'):
        logger.info(f"Fetching data for {asset_name} from Cassandra...")
        cluster = Cluster([self.cassandra_host], port=self.cassandra_port)
        session = cluster.connect(self.keyspace)
        
        query = f"SELECT timestamp, close FROM {self.table} WHERE asset_name = '{asset_name}' ALLOW FILTERING"
        rows = session.execute(query)
        df = pd.DataFrame(rows)
        
        if df.empty:
            raise ValueError(f"No data found for {asset_name} in Cassandra.")
            
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        cluster.shutdown()
        return df

    def prepare_data(self, df, window_size=10):
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(df['close'].values.reshape(-1, 1))
        
        X, y = [], []
        for i in range(window_size, len(scaled_data)):
            X.append(scaled_data[i-window_size:i, 0])
            y.append(scaled_data[i, 0])
            
        X, y = np.array(X), np.array(y)
        X = np.reshape(X, (X.shape[0], X.shape[1], 1))
        
        return X, y, scaler

    def build_model(self, input_shape):
        model = Sequential([
            LSTM(units=50, return_sequences=True, input_shape=input_shape),
            Dropout(0.2),
            LSTM(units=50, return_sequences=False),
            Dropout(0.2),
            Dense(units=1)
        ])
        model.compile(optimizer='adam', loss='mean_squared_error')
        return model

    def train(self, asset_name='bitcoin'):
        with mlflow.start_run():
            df = self.fetch_data(asset_name)
            X, y, scaler = self.prepare_data(df)
            
            # Split data
            train_size = int(len(X) * 0.8)
            X_train, X_test = X[:train_size], X[train_size:]
            y_train, y_test = y[:train_size], y[train_size:]
            
            model = self.build_model((X_train.shape[1], 1))
            
            logger.info("Starting training...")
            model.fit(X_train, y_train, epochs=10, batch_size=32, validation_split=0.1)
            
            # Predictions
            predictions = model.predict(X_test)
            
            # Inverse transform
            y_test_inv = scaler.inverse_transform(y_test.reshape(-1, 1))
            predictions_inv = scaler.inverse_transform(predictions)
            
            # Metrics
            mae = mean_absolute_error(y_test_inv, predictions_inv)
            rmse = np.sqrt(mean_squared_error(y_test_inv, predictions_inv))
            mape = np.mean(np.abs((y_test_inv - predictions_inv) / y_test_inv)) * 100
            
            # Directional Accuracy (Hit Rate)
            actual_diff = np.diff(y_test_inv.flatten())
            pred_diff = np.diff(predictions_inv.flatten())
            hit_rate = np.mean((actual_diff > 0) == (pred_diff > 0)) * 100

            logger.info(f"MAE: {mae}, RMSE: {rmse}, MAPE: {mape}%, Hit Rate: {hit_rate}%")
            
            # MLflow Logging
            mlflow.log_param("asset", asset_name)
            mlflow.log_param("window_size", 10)
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("mape", mape)
            mlflow.log_metric("hit_rate", hit_rate)
            
            from mlflow.models.signature import infer_signature
            signature = infer_signature(X_train, model.predict(X_train))
            
            mlflow.tensorflow.log_model(model, "lstm_model", signature=signature)
            logger.info("Model and metrics logged to MLflow.")

if __name__ == "__main__":
    trainer = LSTMTrainer()
    trainer.train('bitcoin')
