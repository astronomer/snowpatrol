from airflow.decorators import dag, task
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import pickle
from statsmodels.tsa.seasonal import seasonal_decompose
from tempfile import TemporaryDirectory
import wandb

wandb_project='snowstorm'
wandb_entity='snowstorm'

_SNOWFLAKE_CONN_ID = "snowflake_ro"

snowflake_hook = SnowflakeHook(_SNOWFLAKE_CONN_ID)

usage_table = Dataset(uri="USAGE_IN_CURRENCY_DAILY")

feature_table = Dataset(uri="USAGE_FEATURES", 
                        extra={
                            "cutoff_date": "'2022-11-15'",
                            "cost_categories": ["compute", "storage"]
                            })

isolation_forest_model = Dataset(uri="isolation_forest_model", 
                                 extra={
                                     "cost_models": ["total_usage", "compute", "storage"]
                                     })

@dag(
    default_args={},
    schedule=[feature_table, isolation_forest_model],
    start_date=days_ago(2),
    is_paused_upon_creation=False,
)
def predict_isolation_forest():
    """

    
    """

    @task(outlets=[isolation_forest_model])
    def predict(cost_category:str) -> pd.DataFrame:
        """
        

        """  
        
        model_name = f"isolation_forest_{cost_category}"
        
        wandb.login()
        
        with TemporaryDirectory() as model_dir:
        
            artifact = wandb.Api().artifact(
                name=f"{wandb_project}/{wandb_project}/{model_name}:latest", 
                type="model")

            with open(artifact.file(model_dir), 'rb') as mf:
                model = pickle.load(mf)

            usage_df = snowflake_hook.get_pandas_df(
                f"""SELECT "date", "{cost_category}" 
                  FROM {feature_table.uri} 
                  ORDER BY "date" ASC;"""
            )

            usage_df.date = pd.to_datetime(usage_df.date)
            usage_df.set_index('date', inplace=True)
            usage_df.fillna(value=0, inplace=True)
            
            stl = seasonal_decompose(usage_df[cost_category], model='additive', extrapolate_trend="freq")

            usage_stationary = stl.resid.values.reshape(-1,1)

            usage_df["scores"] = model.decision_function(usage_stationary) 
            anomaly_threshold = artifact.metadata.get("anomaly_threshold")
            
            anomalies_df = usage_df.iloc[-5:].loc[(usage_df.scores <= anomaly_threshold) & 
                                                  (usage_df[cost_category] > usage_df[cost_category].mean()), 
                                                  [cost_category]]
            return anomalies_df
        
    @task()
    def generate_report(anomaly_dfs:[pd.DataFrame]) -> pd.DataFrame:

        anomalies_df = pd.concat(anomaly_dfs, axis=0).reset_index()

        report_dates = anomalies_df.date.apply(lambda x: str(x.date())).unique().tolist()

        usage_df = snowflake_hook.get_pandas_df(
                f"""SELECT ACCOUNT_NAME, USAGE_DATE, USAGE_TYPE_CLEAN, USAGE_IN_CURRENCY FROM {usage_table.uri} 
                    WHERE USAGE_DATE IN ({str(report_dates)[1:-1]})
                  ORDER BY "USAGE_DATE" ASC, "USAGE_IN_CURRENCY" DESC;"""
            )
        
        return usage_df.to_html()

        # anomalies_df = pd.DataFrame([{"date": pd.to_datetime('2023-12-04 00:00:00'), "compute": 826},
        # {"date": pd.to_datetime('2023-12-03 00:00:00'), "compute": 800},
        # {"date": pd.to_datetime('2023-12-04 00:00:00'), "storage": 15},
        # {"date": pd.to_datetime('2023-12-04 00:00:00'), "usage_date": 200}])
                
    @task.branch()
    def check_notify(anomaly_dfs:[pd.DataFrame]):
        if len(pd.concat(anomaly_dfs, axis=0)) > 0:
            return ["send_email"]
    
    anomaly_dfs = predict.expand(cost_category=feature_table.extra.get("cost_categories"))

    report = generate_report(anomaly_dfs=anomaly_dfs)

    EmailOperator(
        task_id="send_email",
        to=[],
        subject="Anomalous activity in Snowflake usage",
        html_content=report
    )

predict_isolation_forest()