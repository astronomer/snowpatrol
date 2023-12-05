from airflow.decorators import dag, task
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import numpy as np
import pickle
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.seasonal import seasonal_decompose
from tempfile import TemporaryDirectory
import wandb

wandb_project='snowstorm'
wandb_entity='snowstorm'
threshold_cutoff = 3 #we will assume x std from the mean as anomalous

_SNOWFLAKE_CONN_ID = "snowflake_ro"

feature_table = Dataset(uri="USAGE_FEATURES", 
                        extra={
                            "cutoff_date": "'2022-11-15'",
                            "cost_categories": ["compute", "storage"]
                            })

isolation_forest_model = Dataset(uri="isolation_forest_model", 
                                 extra={
                                     "cost_models": ["compute", "storage"]
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
    def predict(cost_category:str, run_id:str = None):
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
        
            snowflake_hook = SnowflakeHook(_SNOWFLAKE_CONN_ID)

            date, usage = snowflake_hook.get_records(
                f"""SELECT "date", "{cost_category}" 
                  FROM {feature_table.uri} 
                  ORDER BY "date" DESC
                  LIMIT 1;"""
            )[0]
            
            stl = seasonal_decompose(usage_df[cost_category], model='additive')

            trend = stl.trend.fillna(0).values.reshape(-1,1)
            stationary = usage_df[cost_category].values.reshape(-1, 1) - trend

            scaled_usage = StandardScaler().fit_transform(stationary)

            score = model.score_samples(np.array(usage).reshape(-1,1))
            anomaly_threshold = artifact.metadata.get("anomaly_threshold")
            
            anomalies = usage_df.loc[(usage_df["scores"] <= anomaly_threshold) & 
                                     (usage_df[cost_category] > usage_df[cost_category].mean()), [cost_category]]
                
    model_id = predict.expand(cost_category=feature_table.extra.get("cost_categories"))

predict_isolation_forest()