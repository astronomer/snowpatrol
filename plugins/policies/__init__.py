from airflow.models import BaseOperator
from airflow.policies import hookimpl

from dataclasses import dataclass, asdict


@dataclass
class QueryTag:
    dag_id: str = "{{ dag.dag_id }}"
    task_id: str = "{{ task.task_id }}"
    run_id: str = "{{ run_id }}"
    logical_date: str = "{{ logical_date }}"
    started: str = "{{ ti.start_date }}"
    operator: str = "{{ ti.operator }}"

    def __str__(self):
        return str(asdict(self))


SNOWFLAKE_HOOK_PARAMS = {"session_parameters": {"query_tag": str(QueryTag())}}


@hookimpl
def task_policy(task: BaseOperator):
    # If using Airflow < 2.10.0, we need to add the hook_params to the template fields
    task.template_fields = (*task.template_fields, "hook_params")
    # Override hook params to add Snowflake Query Tag
    task.hook_params = (
        {**task.hook_params, **SNOWFLAKE_HOOK_PARAMS} if task.hook_params else SNOWFLAKE_HOOK_PARAMS
    )
