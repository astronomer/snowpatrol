import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pandas as pd
from dash import html


def generate_table(id: str, title: str, dataframe: pd.DataFrame):
    columns = [{"field": item} for item in dataframe.columns]

    return dbc.Row(
        dbc.Col(
            [
                html.H5(children=title),
                html.Div(
                    dag.AgGrid(
                        id=id,
                        columnDefs=columns,
                        rowData=dataframe.to_dict("records"),
                        defaultColDef={
                            "flex": 1,
                            "minWidth": 150,
                            "sortable": True,
                            "resizable": True,
                            "filter": True,
                        },
                        dashGridOptions={"rowSelection": "multiple"},
                    ),
                    className="dbc",
                ),
            ],
            width=12,
        ),
        class_name="bg-light px-1 py-3",
    )
