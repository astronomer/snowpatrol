import plotly.express as px
from flask import Blueprint, render_template

from plugins.snowpatrol.models import Anomaly, Metering

bp = Blueprint("main", __name__)


@bp.route("/", methods=["GET"])
def index():
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
    anomalies = Anomaly.query.order_by(
        Anomaly.prediction_datetime.desc(), Anomaly.warehouse_name, Anomaly.usage_date
    ).all()
    metering = Metering.query.order_by(Metering.usage_date).all()
    column_labels = [str(column).replace("_", " ").title() for column in columns]

    plot_data = get_anomaly_chart(metering, anomalies)

    return render_template(
        "index.html",
        table_columns=column_labels,
        table_data=anomalies,
        plot_data=plot_data,
        zip=zip,
    )


@bp.route("/anomaly/<int:id>", methods=["GET"])
def get_anomaly(id: int):
    data = Metering.query.get(id)
    return render_template("anomalies/table_row.html", row=data)


def get_anomaly_chart(metering, anomalies):
    # Plot the line chart
    fig = px.line(
        x=[x.usage_date for x in metering],
        y=[x.credits_used for x in metering],
        color=[x.warehouse_name for x in metering],
        title="Credits Used Over Time",
        labels={"y": "Credits Used", "x": "Date"},
    )

    # Highlight data points above the compute_threshold in a different color
    if anomalies:  # Assuming anomalies is also a list of dictionaries
        scatter_trace = px.scatter(
            x=[x.usage_date for x in anomalies],
            y=[x.credits_used for x in anomalies],
            color_discrete_sequence=["black"],
        )
        fig.add_trace(scatter_trace.data[0])
    return fig.to_json()
