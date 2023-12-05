from airflow.decorators import dag, task
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import matplotlib.pyplot as plt
import pandas as pd
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
    schedule=None,
    start_date=days_ago(2),
    is_paused_upon_creation=False,
)
def train_isolation_forest():
    """

    
    """

    @task(outlets=[isolation_forest_model])
    def train(cost_category:str, run_id:str = None):
        """
        

        """  

        model_name = f"isolation_forest_{cost_category}"

        wandb.login()
        with TemporaryDirectory() as model_dir, wandb.init(
                project=wandb_project, 
                entity=wandb_entity,
                dir=model_dir,
                name=cost_category,
                group="isolation_forest_"+run_id.replace(':','_'),
                job_type="train_isolation_forest", 
                resume="allow",
                force=True,
                config={
                    "features": feature_table,
                    }):
            
            snowflake_hook = SnowflakeHook(_SNOWFLAKE_CONN_ID)

            usage_df = snowflake_hook.get_pandas_df(
                f"""SELECT "date", "{cost_category}" 
                  FROM {feature_table.uri} 
                  ORDER BY "date" DESC;"""
            )

            usage_df.date = pd.to_datetime(usage_df.date)
            usage_df.set_index('date', inplace=True)
            usage_df.fillna(value=0, inplace=True)
            
            stl = seasonal_decompose(
                usage_df[cost_category], model='additive', extrapolate_trend='freq')
            
            usage_stationary = stl.resid.values.reshape(-1,1)

            model = IsolationForest().fit(usage_stationary)

            usage_df["scores"] = model.decision_function(usage_stationary) 
            anomaly_threshold = usage_df.scores.mean() - (threshold_cutoff * usage_df.scores.std())

            anomalies_df = usage_df.loc[(usage_df.scores <= anomaly_threshold) & 
                                        (usage_df[cost_category] > usage_df[cost_category].mean()), 
                                        [cost_category]]
            anomalies_df

            #save artifacts
            with open(f"{model_dir}/{model_name}.pkl", "wb") as model_file:
                pickle.dump(model, model_file)

            stl.plot().savefig(
                f"{model_dir}/{cost_category}_stl.png", bbox_inches='tight', transparent=True)
            
            fig, ax = plt.subplots(figsize=(10,6))
            _ = ax.plot(
                usage_df.index, usage_df[cost_category], color='black', label = 'Normal')
            _ = ax.scatter(
                anomalies_df.index, anomalies_df[cost_category], color='red', label = 'Anomaly')
            _ = plt.legend()
            fig.savefig(
                f"{model_dir}/{cost_category}_anomalies.png", bbox_inches='tight', transparent=True)
            
            #upload artifacts
            artifact = wandb.Artifact(
                model_name, type="model", metadata={"anomaly_threshold": anomaly_threshold})
            artifact.add_file(local_path=f"{model_dir}/{model_name}.pkl")
            wandb.log_artifact(artifact)
            wandb.run.link_artifact(artifact, f"{wandb_entity}/{wandb_project}/{model_name}:latest")

            wandb.log({"anomalies": wandb.Image(f"{model_dir}/{cost_category}_anomalies.png")})
            wandb.log({"stl": wandb.Image(f"{model_dir}/{cost_category}_stl.png")})
                
    model_id = train.expand(cost_category=feature_table.extra.get("cost_categories"))

train_isolation_forest()