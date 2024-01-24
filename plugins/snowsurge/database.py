from airflow.utils.session import create_session
from flask_appbuilder import SQLA
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

from plugins.snowsurge import bind_name
from plugins.snowsurge.config import snowflake_hook


def attach_bind(app):
    """
    Attach a new Database bind to the Flask app.
    This allows us to create new tables without impacting Airflow's Models.
    In this case, we attach to the same database as the default bind.
    """
    db_uri = app.config["SQLALCHEMY_DATABASE_URI"]
    if app.config["SQLALCHEMY_BINDS"] is None:
        app.config["SQLALCHEMY_BINDS"] = {bind_name: db_uri}
    else:
        app.config["SQLALCHEMY_BINDS"].update({bind_name: db_uri})


def init_db(app, bind):
    db = SQLA(app)
    from plugins.snowsurge.models import AnnotatedAnomaly
    with app.app_context():
        db.create_all(bind=bind)
        #update_anomalies()


def update_anomalies():
    """
    Update the AnnotatedAnomaly table with the latest anomalies from Snowflake.
    """
    from plugins.snowsurge.models import AnnotatedAnomaly

    metadata = MetaData()
    engine = snowflake_hook.get_sqlalchemy_engine()

    # Reflect the table using SQLAlchemy Core
    model_output_anomalies = Table(
        "MODEL_OUTPUT_ANOMALIES", metadata, autoload_with=engine, schema="snowstorm"
    )

    # Snowflake stores all case-insensitive object names in uppercase text.
    # In contrast, SQLAlchemy considers all lowercase object names to be case-insensitive.
    # Snowflake SQLAlchemy converts the object name case during schema-level communication.
    # https://github.com/snowflakedb/snowflake-sqlalchemy#object-name-case-handling

    columns = [
        model_output_anomalies.c.usage_date,
        model_output_anomalies.c.warehouse_name,
        model_output_anomalies.c.credits_used,
    ]

    query = model_output_anomalies.select().with_only_columns(columns)
    records = engine.execute(query).fetchall()

    anomalies = []
    for row in records:
        anomaly = {
            "usage_date": row[0],
            "warehouse_name": row[1],
            "credits_used": row[2],
            "is_valid": False,
            "comment": "",
        }
        anomalies.append(anomaly)

    # Use Core's insert statement
    statement = insert(AnnotatedAnomaly).values(anomalies)
    # If the Anomaly already exists, we skip it.
    statement = statement.on_conflict_do_nothing(
        index_elements=["usage_date", "warehouse_name"],
    )

    with create_session() as session:
        session.execute(statement)
