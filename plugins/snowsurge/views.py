from flask_appbuilder import ModelView
from flask_appbuilder.models.sqla.interface import SQLAInterface

from plugins.snowsurge.models import AnnotatedAnomaly


class AnnotatedAnomalyModelView(ModelView):
    """View to show the Anomalies and allow users to annotate them"""

    route_base = "/anomalies"

    datamodel = SQLAInterface(AnnotatedAnomaly)

    # TODO: Handle permissions

    list_columns = [
        "warehouse_name",
        "usage_date",
        "credits_used",
        "is_valid",
        "comment",
    ]

    search_columns = ["warehouse_name", "usage_date", "is_valid", "comment"]

    edit_columns = ["is_valid", "comment"]

    base_order = ("usage_date", "desc")

    # TODO: May have to define edit_form and formatters_columns
