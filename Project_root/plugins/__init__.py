from airflow.plugins_manager import AirflowPlugin
from plugins.custom_operators import CustomTransformOperator
from plugins.data_quality import DataQualityCheckOperator
from plugins.anomaly_detection import AnomalyDetectionOperator

class ETLPlugin(AirflowPlugin):
    name = "etl_plugin"
    operators = [CustomTransformOperator, DataQualityCheckOperator, AnomalyDetectionOperator]
