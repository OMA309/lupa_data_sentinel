import pandas as pd

def analyze_root_cause(df: pd.DataFrame) -> dict:
    """
    Simple root cause analysis for missing/duplicate issues.
    """
    issues = {}
    if df.duplicated().any():
        issues["duplicates"] = "Data source may have repeated records."
    if df.isnull().any().any():
        issues["missing"] = "Possible data ingestion failure or incomplete source extract."
    return issues