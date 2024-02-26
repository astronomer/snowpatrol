import plotly.express as px


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
