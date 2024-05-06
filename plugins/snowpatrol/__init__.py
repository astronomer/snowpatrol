from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from dash import Input, Output

from plugins.snowpatrol.layout import dashboard_layout
from plugins.snowpatrol.view import SnowPatrol

MENU = "SnowPatrol"
MENU_ITEM = "View 📊️ Dashboard"
URL_PREFIX = "snowpatrol"

base_url = conf.get("webserver", "base_url")

v_appbuilder_view = SnowPatrol()

v_appbuilder_view.layout = dashboard_layout


@v_appbuilder_view.callback(Output("output", "children"), [Input("button", "n_clicks")])
def update_output(n_clicks):
    if n_clicks is None:
        return "Button not clicked"
    return f"Button clicked {n_clicks} times"


v_appbuilder_package = {
    "name": MENU_ITEM,
    "category": MENU,
    "view": v_appbuilder_view,
}


class SnowpatrolPlugin(AirflowPlugin):
    name = "snowpatrol"
    appbuilder_views = [v_appbuilder_package]
