import dash_bootstrap_components as dbc
from dash import dcc, html

from plugins.snowpatrol.components.table import generate_table
from plugins.snowpatrol.datasets import (
    temp_df,
)  # dag_task_cost_df, dag_execution_cost_df, dag_time_series_df,

layout = html.Div(
    [
        dbc.Container(
            dbc.Stack(
                [
                    dbc.Row(),
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Div(
                                    [
                                        html.Label("Date Range"),
                                        dcc.DatePickerRange(
                                            id="date-picker-range",
                                            start_date_placeholder_text="Start Date",
                                            end_date_placeholder_text="End Date",
                                            calendar_orientation="horizontal",
                                        ),
                                    ],
                                    style={"text-align": "left"},
                                ),
                                width=4,
                                align="center",
                            ),
                            dbc.Col(
                                html.Div(
                                    [
                                        html.Label("Airflow Dag"),
                                        dcc.Dropdown(
                                            id="dag-id-dropdown",
                                            multi=True,
                                            options=[],
                                            value="",
                                        ),
                                    ]
                                ),
                                width=4,
                            ),
                            dbc.Col(
                                html.Div(
                                    [
                                        html.Label("Airflow Task"),
                                        dcc.Dropdown(
                                            id="task-id-dropdown",
                                            multi=True,
                                            options=[],
                                            value="",
                                        ),
                                    ]
                                ),
                                width=4,
                            ),
                        ],
                        class_name="bg-light px-1 py-3",
                    ),
                    generate_table(
                        id="airflow-dag-and-task-costs",
                        title="Airflow Dag and Task Costs",
                        dataframe=temp_df,
                    ),
                    generate_table(
                        id="airflow-dag-execution-cost",
                        title="Airflow DAG Execution Cost",
                        dataframe=temp_df,
                    ),
                ],
                gap=3,
            )
        )
    ]
)
