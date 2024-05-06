import logging

from airflow.security import permissions
from airflow.www.app import csrf
from airflow.www.auth import has_access
from dash import Dash
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose


class SnowPatrol(AppBuilderBaseView, Dash):
    # AppBuilderBaseView Config
    template_folder = "templates"  # Template folder relative location
    static_folder = "static"  # Static folder relative location
    default_view = "index"  #  Default view for this BaseView

    # Dash Config
    _index_string = ""
    _layout = None

    def __init__(self, **kwargs):
        self._urls = None
        # Initialize Dash without a Server first
        Dash.__init__(
            self,
            name="Dash",
            server=False,
            url_base_pathname="/dash/",
            **kwargs,
        )
        AppBuilderBaseView.__init__(self)
        logging.debug(
            "========== SUCCESSFULLY LOADED PLUGIN SnowPatrolAppBuilderView =========="
        )

    def _add_url(self, name, view_func, methods=("GET",)):
        """
        Override Dash _add_url to register Dash URLs to AppBuilderBaseView
        """
        csrf.exempt(view_func)
        full_name = self.config.routes_pathname_prefix + name

        logging.debug(f"Adding URL {name} to AppBuilderBaseView")
        self._urls.append(name, view_func.__name__, methods)

        # record the url in Dash.routes so that it can be accessed later
        # e.g. for adding authentication with flask_login
        self.routes.append(full_name)

    def interpolate_index(
        self,
        metas="",
        title="",
        css="",
        config="",
        scripts="",
        app_entry="",
        favicon="",
        renderer="",
    ):
        """
        Override Dash to use Airflow's base template.
        We render the template with AppBuilderBaseView's render_template function before passing it to Dash
        """
        return self.render_template(
            "index.html", app_entry=app_entry, dash_config=config, scripts=scripts
        )

    @expose("/")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def index(self):
        return Dash.index(self)
