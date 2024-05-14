from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access
from flask import Blueprint
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose

# Define the Flask blueprint
bp = Blueprint(
    "snowpatrol",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/snowpatrol",
)


class Snowpatrol(AppBuilderBaseView):
    default_view = "main"

    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    @expose("/")
    def main(self):
        return self.render_template("index.html")


v_appbuilder_view = Snowpatrol()

v_appbuilder_package = {
    "name": "View 📊 Dashboards",
    "category": "SnowPatrol",
    "view": v_appbuilder_view,
}


class SnowpatrolAnnotationPlugin(AirflowPlugin):
    name = "snowpatrol"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
