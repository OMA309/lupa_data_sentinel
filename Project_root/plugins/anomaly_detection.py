from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.anomaly_detections import detect_anomalies
import pandas as pd

class AnomalyDetectionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, data_source, numeric_cols=None, contamination=0.05, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source = data_source
        self.numeric_cols = numeric_cols
        self.contamination = contamination

    def execute(self, context):
        self.log.info(f"Running anomaly detection on {self.data_source}...")

        # Pull dataframe from XCom
        df: pd.DataFrame = context['ti'].xcom_pull(task_ids=self.data_source)

        if df is None or df.empty:
            self.log.warning("No data received for anomaly detection.")
            return {"anomaly_flag": []}

        # Call your src.anomaly_detection logic
        result_df = detect_anomalies(df, numeric_cols=self.numeric_cols, contamination=self.contamination)

        anomalies = result_df[result_df["anomaly_flag"]].index.tolist()
        self.log.info(f"Detected {len(anomalies)} anomalies.")

        return {"anomalies": anomalies, "total": len(df), "num_anomalies": len(anomalies)}
