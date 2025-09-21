# dags/auto_lineage_tracking.py

import json
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.models.baseoperator import BaseOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

LINEAGE_FILE = "/opt/airflow/logs/lineage_history.json"

def record_task_lineage(task_instance: TaskInstance, dag_run: DagRun):
    """
    Records the task output along with upstream/downstream dependencies
    in a JSON file for DAG-level lineage tracking.
    """
    task: BaseOperator = task_instance.task

    # Capture upstream and downstream task_ids
    upstream = [t.task_id for t in task.upstream_list]
    downstream = [t.task_id for t in task.downstream_list]

    # Capture XCom output for this task (if any)
    try:
        output = task_instance.xcom_pull(task_ids=task.task_id)
    except Exception:
        output = None

    lineage_record = {
        "dag_id": dag_run.dag_id,
        "dag_run_id": dag_run.run_id,
        "execution_date": str(dag_run.execution_date),
        "task_id": task.task_id,
        "upstream": upstream,
        "downstream": downstream,
        "output": output
    }

    # Load existing lineage history
    try:
        with open(LINEAGE_FILE, "r") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []

    # Append new record and save
    history.append(lineage_record)
    with open(LINEAGE_FILE, "w") as f:
        json.dump(history, f, indent=4)

    print(f"[Lineage] Recorded task {task.task_id} for DAG {dag_run.dag_id}")

# Global callback that applies to every task in the DAG
def lineage_callback(context):
    ti: TaskInstance = context['ti']
    dr: DagRun = context['dag_run']
    record_task_lineage(ti, dr)

# Example DAG using auto lineage tracking
with DAG(
    dag_id="example_auto_lineage_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    def task_a_func():
        return "Task A output"

    def task_b_func():
        return "Task B output"

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=task_a_func,
        on_success_callback=lineage_callback  # Automatically tracks this task
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=task_b_func,
        on_success_callback=lineage_callback  # Automatically tracks this task
    )

    task_a >> task_b
