import pandas as pd
import os
import logging

def load_raw(df: pd.DataFrame, path: str):
    """Save snapshot in raw/original format (CSV)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logging.info(f"Raw data saved to {path}")

def load_clean(df: pd.DataFrame, path: str):
    """Save cleaned dataset for downstream use."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logging.info(f"Clean data saved to {path}")