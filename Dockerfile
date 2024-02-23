# syntax=quay.io/astronomer/airflow-extensions:v1

FROM quay.io/astronomer/astro-runtime:9.8.0

PYENV 3.11 evidently_venv requirements_evidently.txt
