# from labeller.models import db
# from labeller.forms import CreateAnomalyForm

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.security import permissions
from airflow.www.auth import has_access
from flask import Blueprint
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from sqlalchemy.orm import sessionmaker as SQLAlchemySessionMaker

from plugins.snowpatrol.charts import get_anomaly_chart
from plugins.snowpatrol.models import Anomaly, Metering

# Postgres Configuration
postgres_conn_id = "postgres_admin"
postgres_hook = PostgresHook(postgres_conn_id)

# Define the Flask blueprint
bp = Blueprint(
    "snowpatrol",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/snowpatrol",
)

engine = postgres_hook.get_sqlalchemy_engine()
Session = SQLAlchemySessionMaker(engine)


class Snowpatrol(AppBuilderBaseView):
    default_view = "main"

    @expose("/", methods=["GET"])
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def main(self):
        """
        Renders the index page
        """

        columns = [
            "id",
            "prediction_datetime",
            "warehouse_name",
            "usage_date",
            "credits_used",
            "trend",
            "seasonal",
            "residual",
            "score",
        ]
        with Session() as session:
            anomalies = (
                session.query(Anomaly)
                .order_by(
                    Anomaly.prediction_datetime.desc(),
                    Anomaly.warehouse_name,
                    Anomaly.usage_date,
                )
                .all()
            )
            metering = session.query(Metering).order_by(Metering.usage_date).all()
        column_labels = [str(column).replace("_", " ").title() for column in columns]

        plot_data = get_anomaly_chart(metering, anomalies)

        return self.render_template(
            "index.html",
            table_columns=column_labels,
            table_data=anomalies,
            plot_data=plot_data,
            zip=zip,
        )

    @expose("/anomaly/<int:id>", methods=["GET"])
    def get_anomaly(self, id: int):
        data = Metering.query.get(id)
        return self.render_template("anomalies/table_row.html", row=data)


v_appbuilder_view = Snowpatrol()

v_appbuilder_package = {
    "name": "View ❄️ Anomalies",
    "category": "Snowpatrol",
    "view": v_appbuilder_view,
}


class SnowpatrolAnnotationPlugin(AirflowPlugin):
    name = "snowpatrol"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
