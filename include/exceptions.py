from airflow import AirflowException


class DataValidationFailed(AirflowException):
    """Raise when Data validation failed."""
