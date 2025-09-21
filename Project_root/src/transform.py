import pandas as pd
import logging

def clean_and_validate(df: pd.DataFrame) -> pd.DataFrame:
    """Basic data cleaning and validation."""
    logging.info("Starting data cleaning and validation")

    # Drop duplicates
    df = df.drop_duplicates()

    # Handle missing values (simple fill, can be replaced with advanced logic)
    df = df.fillna("MISSING")

    # Enforce consistent formatting for column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    logging.info("Data cleaning and validation complete")
    return df