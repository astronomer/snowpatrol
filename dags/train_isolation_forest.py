from airflow.decorators import dag, task
from airflow import Dataset
from airflow.utils.dates import days_ago
import datetime
from snowpark_provider import SnowparkTable


_SNOWFLAKE_CONN_ID = "snowflake_admin"

feature_table = Dataset(uri="FEATURES")

cutoff_date = datetime.date(2022, 11, 15)

cost_categories = ["compute", "storage"]

@dag(
    default_args={},
    schedule=[feature_table],
    start_date=days_ago(2),
)
def feature_engineering():
    """

    
    """

    @task.snowpark_python(snowflake_conn_id=_SNOWFLAKE_CONN_ID, outlets=)
    def train(feature_table: SnowparkTable, cost_category: str):
        """
        

        """  
        from snowpark_provider.hooks.snowpark import SnowparkHook
        from statsmodels.tsa.seasonal import seasonal_decompose
        from sklearn.ensemble import IsolationForest

        snowpark_session = SnowparkHook(_SNOWFLAKE_CONN_ID).get_conn()        
        features_df = snowpark_session.table(feature_table.name).to_pandas()
        # features_df = feature_table.to_pandas()

        stl = seasonal_decompose(features_df.compute, model='additive')




    load_account_data(
        feature_table=SnowparkTable(name=feature_table.uri),
        currency_usage=SnowparkTable(name=currency_table.uri),
        cutoff_date=cutoff_date
        )

feature_engineering()