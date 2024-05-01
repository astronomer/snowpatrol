from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from plugins.snowpatrol.view import DashView

bp = Blueprint(
    "snowpatrol",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/snowpatrol",
)

dash_view = DashView()

appbuilder_package = {
    "name": "View 📊️ Dashboard",
    "category": "Snowpatrol",
    "view": dash_view,
}


class SnowpatrolAnnotationPlugin(AirflowPlugin):
    name = "snowpatrol"
    flask_blueprints = [bp]
    appbuilder_views = [appbuilder_package]
