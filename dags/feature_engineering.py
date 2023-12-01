from airflow.decorators import dag, task
from airflow import Dataset
from airflow.utils.dates import days_ago
import datetime
from snowpark_provider import SnowparkTable


_SNOWFLAKE_CONN_ID = "snowflake_admin"

currency_table = Dataset(uri="USAGE_IN_CURRENCY_DAILY")
feature_table = Dataset(uri="FEATURES")

cutoff_date = datetime.date(2022, 11, 15)

@dag(
    default_args={},
    schedule=[currency_table],
    start_date=days_ago(2),
)
def feature_engineering():
    """
    This DAG performs feature engineering
    """

    @task.snowpark_python(snowflake_conn_id=_SNOWFLAKE_CONN_ID, outlets=feature_table)
    def build_features(feature_table: SnowparkTable, currency_usage: SnowparkTable, cutoff_date):
        """


        

        """  
        import pandas as pd
        import datetime

        usage = currency_usage.filter(
            (F.col('USAGE_DATE') >= cutoff_date) &
            (F.col('USAGE_DATE') < datetime.date.today()))
                                                
        pivot_values = usage.select('USAGE_TYPE').distinct().to_pandas().USAGE_TYPE.to_list()
        
        usage_df = usage.select('USAGE_DATE', 'USAGE_TYPE', 'USAGE')\
                                .pivot(pivot_col='USAGE_TYPE', values=pivot_values)\
                                .sum('USAGE')\
                                .sort('USAGE_DATE')\
                                .to_pandas()

        usage_df.rename()
        usage_df.columns = ['date']+pivot_values
        usage_df.date = pd.to_datetime(usage_df.date)
        usage_df.set_index('date', inplace=True)
        usage_df.fillna(value=0, inplace=True)
        usage_df = usage_df.apply(pd.to_numeric, downcast='float')

        snowpark_session.write_pandas(usage_df, feature_table.table_name, auto_create_table=True, overwrite=True)


    build_features(
        feature_table=SnowparkTable(name=feature_table.uri),
        currency_usage=SnowparkTable(name=currency_table.uri),
        cutoff_date=cutoff_date
        )

feature_engineering()