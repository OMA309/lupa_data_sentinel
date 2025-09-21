
import streamlit as st
import pandas as pd
import json
import os
import psycopg2
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
import logging
from requests.auth import HTTPBasicAuth  # NEW

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


st.set_page_config(
    page_title="Lupa - Data Sentinel",
    page_icon="🐺",
    layout="wide"
)

STAGING_DIR = "/data/staging"
ARCHIVE_DIR = "/data/archive"
os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres_contain'),
    'port': os.getenv('DB_PORT', '5432'),
    'user': os.getenv('DB_USER', 'datauser'),
    'password': os.getenv('DB_PASSWORD', 'datapass'),
    'database': os.getenv('DB_NAME', 'data_sentinel')
}

# Airflow config
AIRFLOW_BASE = os.getenv('AIRFLOW_API_URL', 'http://airflow-webserver:8080/api/v1')
AIRFLOW_USER = os.getenv('AIRFLOW_USERNAME', 'apiuser')   
AIRFLOW_PASS = os.getenv('AIRFLOW_PASSWORD', 'apipass')   
AUTH = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS)


def get_db_engine():
    """Get database engine"""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string, pool_pre_ping=True)

def test_database_connection():
    """Test database connection"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)


def trigger_airflow_dag(dataset_name, file_path):
    """Trigger Airflow DAG for data processing"""
    dag_id = "data_sentinel_pipeline"   # 👈 updated to match your curl example
    url = f"{AIRFLOW_BASE}/dags/{dag_id}/dagRuns"

    payload = {
        "dag_run_id": f"streamlit_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "conf": {
            "filename": os.path.basename(file_path),
            "file_path": file_path,
            "dataset_name": dataset_name,
            "triggered_by": "streamlit_app",
            "timestamp": datetime.now().isoformat()
        }
    }

    try:
        response = requests.post(url, auth=AUTH, json=payload)
        if response.status_code in [200, 201]:
            return True, f"DAG triggered: {response.json().get('dag_run_id', payload['dag_run_id'])}"
        else:
            return False, f"Failed to trigger DAG: {response.text}"
    except Exception as e:
        return False, f"Error contacting Airflow: {str(e)}"

def get_latest_dag_status():
    """Fetch latest DAG run status"""
    dag_id = "data_sentinel_pipeline"
    url = f"{AIRFLOW_BASE}/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(url, auth=AUTH)
        if response.status_code == 200:
            runs = response.json().get("dag_runs", [])
            if runs:
                latest = runs[0]
                return True, f"Latest run {latest['dag_run_id']} → {latest['state']}"
            else:
                return True, "No DAG runs yet."
        else:
            return False, f"Error fetching DAG runs: {response.text}"
    except Exception as e:
        return False, f"Error contacting Airflow: {str(e)}"


def load_file(file):
    """Load file based on extension"""
    filename = file.name.lower()
    if filename.endswith(".csv"):
        return pd.read_csv(file)
    elif filename.endswith((".xlsx", ".xls")):
        return pd.read_excel(file)
    elif filename.endswith(".json"):
        return pd.read_json(file)
    elif filename.endswith(".parquet"):
        return pd.read_parquet(file)
    elif filename.endswith((".txt", ".tsv")):
        return pd.read_csv(file, sep="\t")
    else:
        raise ValueError("Unsupported file format")

def normalize_dataframe(df):
    """Normalize DataFrame columns and basic cleaning"""
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df.dropna(how='all')

def save_to_staging(df, original_filename):
    """Save DataFrame to staging directory"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_name = os.path.splitext(original_filename)[0]
    staging_filename = f"{dataset_name}_{timestamp}.csv"
    staging_path = os.path.join(STAGING_DIR, staging_filename)
    df.to_csv(staging_path, index=False)
    return staging_path, dataset_name


st.sidebar.image("https://i.ibb.co/YX6RrFf/3d-wolf.png", width=120)
st.sidebar.markdown(
    "<h1 style='text-align:center; font-family:Arial Black; color:#2c3e50;'>Lupa</h1>",
    unsafe_allow_html=True
)
st.sidebar.caption("🐺 Resolute AI-powered Data Guardian")

# Database Connection Test
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔗 Database Connection")

if st.sidebar.button("🔍 Test Database Connection", use_container_width=True):
    with st.spinner("Testing connection..."):
        success, message = test_database_connection()
        st.sidebar.success(message) if success else st.sidebar.error(message)

with st.sidebar.expander("Database Configuration"):
    st.code(f"""
Host: {DB_CONFIG['host']}
Port: {DB_CONFIG['port']}
Database: {DB_CONFIG['database']}
User: {DB_CONFIG['user']}
    """)

# File Upload
st.sidebar.markdown("---")
uploaded_file = st.sidebar.file_uploader(
    "📂 Import Dataset",
    type=["csv", "xlsx", "xls", "json", "parquet", "txt"],
    help="Upload your dataset for AI-powered quality analysis"
)

# Airflow controls
st.sidebar.markdown("---")
if st.sidebar.button("🚀 Trigger ETL Pipeline"):
    ok, msg = trigger_airflow_dag("manual_trigger", "N/A")
    st.sidebar.success(msg) if ok else st.sidebar.error(msg)

if st.sidebar.button("📡 Check DAG Status"):
    ok, msg = get_latest_dag_status()
    st.sidebar.info(msg) if ok else st.sidebar.error(msg)


st.title("🐺 Lupa - AI Data Quality Sentinel")

if uploaded_file:
    try:
        df = load_file(uploaded_file)
        df = normalize_dataframe(df)
        st.success(f"✅ File '{uploaded_file.name}' uploaded successfully!")

        # Stats
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Rows", df.shape[0])
        col2.metric("Columns", df.shape[1])
        col3.metric("Missing Values", int(df.isnull().sum().sum()))
        col4.metric("Duplicate Rows", int(df.duplicated().sum()))

        # Preview
        with st.expander("🔍 Preview Uploaded Data", expanded=False):
            st.dataframe(df.head(20), use_container_width=True)

        staging_path, dataset_name = save_to_staging(df, uploaded_file.name)
        st.info(f"📁 Saved to staging: `{staging_path}`")

        # Trigger DAG button
        if st.button("🚀 Start AI Quality Analysis", type="primary"):
            with st.spinner("Triggering AI pipeline..."):
                ok, msg = trigger_airflow_dag(dataset_name, staging_path)
                st.success(msg) if ok else st.error(msg)

    except Exception as e:
        st.error(f"❌ Error processing file: {e}")
        logger.error(f"Error: {e}")

else:
    st.markdown("## Welcome to Lupa 🐺\nUpload a dataset from the sidebar to begin.")


st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:gray;'>Built with ❤️ | Project Lupa 🐺</div>",
    unsafe_allow_html=True
)
