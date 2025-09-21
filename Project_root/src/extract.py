import pandas as pd
import sqlalchemy
import logging

def extract_from_csv(file_path: str) -> pd.DataFrame:
    """Extract data from a CSV file."""
    logging.info(f"Extracting data from CSV: {file_path}")
    return pd.read_csv(file_path)

def extract_from_db(conn_string: str, query: str) -> pd.DataFrame:
    """Extract data from a database using SQLAlchemy."""
    logging.info(f"Extracting data from DB with query: {query}")
    engine = sqlalchemy.create_engine(conn_string)
    return pd.read_sql(query, engine)

def extract_from_api(api_func) -> pd.DataFrame:
    """
    Extract data from an API.
    api_func should be a function that returns JSON data.
    """
    logging.info("Extracting data from API")
    data = api_func()
    return pd.DataFrame(data)