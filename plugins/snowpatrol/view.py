from airflow.www.app import cached_app
from dash import Dash
from flask_appbuilder import BaseView as AppBuilderBaseView, expose


class DashView(AppBuilderBaseView, Dash):
    _index_string = ""
    _layout = None


    def __init__(self, url="/dash/", **kwargs):
        AppBuilderBaseView.__init__(self)
        Dash.__init__(self, server=cached_app(), url_base_pathname=url, name="my_dash_app", **kwargs)

    @expose("/")
    def index(self):
        """
        AppBuilderBaseView Route
        Serve the default Dash Index page
        """
        return Dash.index(self)
