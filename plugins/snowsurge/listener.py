import logging

from airflow.listeners import hookimpl
from airflow.models import TaskInstance
from airflow.utils.state import TaskInstanceState


@hookimpl
def on_task_instance_success(
    previous_state: TaskInstanceState, task_instance: TaskInstance, session
):
    """
    This listener is called every time a task instance succeeds.
    We use it to update the AnnotatedAnomalies table after the predict_isolation_forest Dag has completed.
    This ensures that the AnnotatedAnomalies table is always up-to-date.
    """
    if task_instance.task_id == "predict_isolation_forest":
        logging.info("Updating AnnotatedAnomalies table")
        update_anomalies()
