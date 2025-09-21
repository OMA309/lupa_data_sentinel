import pandas as pd
from pyod.models.iforest import IForest
import logging

def detect_anomalies(df: pd.DataFrame, numeric_cols=None, contamination=0.05, random_state=42) -> pd.DataFrame:
    """Detect anomalies in numeric columns using Isolation Forest."""
    if numeric_cols is None:
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    if not numeric_cols:
        df["anomaly_flag"] = False
        return df

    logging.info(f"Running anomaly detection on columns: {numeric_cols}")
    model = IForest(contamination=contamination, random_state=random_state)
    preds = model.fit_predict(df[numeric_cols].fillna(0))  # fillna to handle missing values

    df["anomaly_flag"] = preds == -1  # -1 = anomaly
    return df