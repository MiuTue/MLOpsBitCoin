import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
import sys
import os

# Ensure we can import from the same directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# We need to mock these before importing if they are used at top level or in __init__
# In train_lstm.py, they are imported at top level.
# references to them in code will use the real modules unless we patch where they are used.

from train_lstm import LSTMTrainer

class TestLSTMTrainer(unittest.TestCase):

    @patch('train_lstm.mlflow')
    @patch.dict('os.environ', {
        'ASSET_CASSANDRA_HOST': 'localhost',
        'ASSET_CASSANDRA_PORT': '9042',
        'ASSET_CASSANDRA_KEYSPACE': 'assets',
        'ASSET_CASSANDRA_TABLE': 'assets',
        'MLFLOW_TRACKING_URI': 'http://localhost:5000'
    })
    def test_prepare_data(self, mock_mlflow):
        """Test that data preparation correctly shapes the input for LSTM"""
        # Mocking mlflow to avoid server connection attempts during init
        trainer = LSTMTrainer()
        
        # Create dummy dataframe with predictable data
        # 20 points, linear increase
        data = {
            'timestamp': pd.date_range(start='1/1/2022', periods=20),
            'close': np.linspace(100, 200, 20)
        }
        df = pd.DataFrame(data)
        
        # Test prepare_data with small window
        window_size = 10
        X, y, scaler = trainer.prepare_data(df, window_size=window_size)
        
        # Expected shapes:
        # Total samples = 20
        # Window size = 10
        # Resulting samples = Total - Window = 10
        
        self.assertEqual(len(X), 10)
        self.assertEqual(len(y), 10)
        
        # X shape: (samples, time_steps, features)
        self.assertEqual(X.shape, (10, 10, 1))
        
        # y shape: (samples,)
        self.assertEqual(y.shape, (10,))
        
        # Verify scaling (min-max scaling should be between 0 and 1)
        self.assertTrue(np.all(X >= 0))
        self.assertTrue(np.all(X <= 1))
        
        # Verify simple logic: since data is increasing, X[0] (first window) should predict y[0] (next value)
        # X[0] is data[0:10], y[0] is data[10]
        # In normalized terms
        # Last element of first window
        last_in_window = X[0][-1][0]
        target = y[0]
        
        # Since linear increase, target should be greater than last in window
        self.assertGreater(target, last_in_window)

if __name__ == '__main__':
    unittest.main()
