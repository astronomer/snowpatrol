from dash import html

dashboard_layout = html.Div(
    [html.Button("Click Me", id="button"), html.Div(id="output")]
)
