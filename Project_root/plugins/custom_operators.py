from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CustomTransformOperator(BaseOperator):
    @apply_defaults
    def __init__(self, transform_func, upstream_task_id='extract_task', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transform_func = transform_func
        self.upstream_task_id = upstream_task_id

    def execute(self, context):
        self.log.info("Running custom transformation...")
        data = context['ti'].xcom_pull(task_ids=self.upstream_task_id)
        if data is None:
            raise ValueError(f"No data found from task {self.upstream_task_id}")
        try:
            transformed = self.transform_func(data)
        except Exception as e:
            self.log.error(f"Transformation failed: {e}")
            raise
        self.log.info("Transformation complete")
        return transformed