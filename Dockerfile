# syntax=quay.io/astronomer/airflow-extensions:v1

FROM quay.io/astronomer/astro-runtime:11.0.0

COPY plugins plugins
RUN pip install ./plugins

PYENV 3.11 evidently_venv requirements_evidently.txt
