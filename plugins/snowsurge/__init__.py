# from time import sleep

from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
# from airflow.www.app import cached_app
from airflow.www.auth import has_access
from flask import Blueprint
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose

from plugins.snowsurge import listener
from plugins.snowsurge.config import bind_name
from plugins.snowsurge.database import attach_bind, init_db, update_anomalies

from plugins.snowsurge.charts import get_anomaly_chart
from plugins.snowsurge.views import AnnotatedAnomalyModelView


# Define the Flask blueprint
bp = Blueprint(
    "snowsurge",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/snowsurge",
)


class Snowsurge(AppBuilderBaseView):
    default_view = "main"

    @expose("/", methods=["GET", "POST"])
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def main(self):
        # session.update(request.form)
        plotly_data = get_anomaly_chart()
        return self.render_template("main.html", plotly_data=plotly_data, zip=zip)


v_appbuilder_view = Snowsurge()

view_package = {
    "name": "View ❄️ Anomalies",
    "category": "Snowsurge",
    "view": v_appbuilder_view,
}


class SnowsurgePlugin(AirflowPlugin):
    name = "snowsurge_plugin"
    hooks = []
    macros = []
    flask_blueprints = [bp]
    appbuilder_views = [view_package]
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
    timetables = []
    listeners = [listener]

    @classmethod
    def on_load(cls, *args, **kwargs):
        pass
        # print("==================== Loading Snowsurge Plugin ===================")
        # # Attach Snowflake database to Airflow Flask App
        # sleep(10)
        # app = cached_app()
        # attach_bind(app)
        # # Initialize the Snowflake database
        # init_db(app, bind_name)
        # # Update the AnnotatedAnomaly table with the latest anomalies from Snowflake
        #
        # print("================= Done Loading Snowsurge Plugin =================")
