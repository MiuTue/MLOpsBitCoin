from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow.tensorflow
import numpy as np
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Binance LSTM Serving API")

# Model configuration
MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MODEL_NAME = "lstm_model"
MODEL_STAGE = "Production" # Or use run ID

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

class PredictionRequest(BaseModel):
    data: list # Expected 10 historical prices

model = None

def load_model():
    global model
    try:
        # Load the latest model from MLflow
        # For simplicity, we'll try to load from a specific run or the latest one
        model_uri = f"models:/{MODEL_NAME}/latest"
        logger.info(f"Loading model from {model_uri}...")
        model = mlflow.tensorflow.load_model(model_uri)
        logger.info("Model loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load model: {str(e)}")
        # Fallback for initial run
        model = None

@app.on_event("startup")
def startup_event():
    load_model()

@app.get("/health")
def health():
    return {"status": "healthy", "model_loaded": model is not None}

@app.post("/predict")
def predict(request: PredictionRequest):
    if model is None:
        load_model()
        if model is None:
            raise HTTPException(status_code=503, detail="Model not loaded yet")
    
    try:
        # Preprocessing: Convert list to numpy and reshape for LSTM
        input_data = np.array(request.data).reshape(1, len(request.data), 1)
        prediction = model.predict(input_data)
        return {"prediction": float(prediction[0][0])}
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
