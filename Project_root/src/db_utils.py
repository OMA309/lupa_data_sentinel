# db_utils.py
import pandas as pd
from sqlalchemy import create_engine

def get_db_engine(user='airflow', password='airflow', host='postgres', port=5432, db='airflow'):
    """
    Returns a SQLAlchemy engine for the PostgreSQL database.
    """
    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_str)
    return engine

def load_latest_table(table_name, engine=None):
    """
    Loads the entire table from the database into a Pandas DataFrame.
    """
    if engine is None:
        engine = get_db_engine()
    
    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(query, engine)
    return df
