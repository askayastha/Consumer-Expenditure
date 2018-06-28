import numpy as np
import pandas as pd
from scipy.interpolate import UnivariateSpline
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from dash.dependencies import Input, Output

app = dash.Dash()

test_pipe = pd.read_csv('/Volumes/Transcend/pumd_data_files/processed_data_5yrs_bucket_jun26/mtbi_avg_spend_intrvw_2011_to_2015.csv')
reshaped_test_pipe = pd.read_csv('/Volumes/Transcend/pumd_data_files/processed_data_5yrs_bucket_jun26/mtbi_reshaped_avg_spend_intrvw_2011_to_2015.csv')

# ucc_pipe = reshaped_test_pipe[['UCC', 'UCC_DESCRIPTION']]
reshaped_test_pipe['UCC_DESCRIPTION'].fillna(reshaped_test_pipe['UCC'].astype(str), inplace=True)
ucc_dict = pd.Series(reshaped_test_pipe['UCC_DESCRIPTION'].values, index=reshaped_test_pipe['UCC']).to_dict()

# test_pipe = test_pipe[test_pipe['UCC'] == 830101]
# x = test_pipe['AGE_REF']
# y = test_pipe['AVG_SPEND']
#
# for i in range(0, 99999999, 1000):
#     u_spline = UnivariateSpline(x, y, s=i)
#     knot_count = len(u_spline.get_knots())
#     print(knot_count)
#     if knot_count <= 8:
#         break
# xs = np.linspace(20, 80, 1000)


app.layout = html.Div([
    html.Div([
        html.Label('Category'),
        dcc.Dropdown(
            id='dropdown-ucc',
            options=[{'label': val, 'value': key} for key, val in ucc_dict.items()],
            value=reshaped_test_pipe['UCC'][0]
        ),
        dcc.RadioItems(
            id='years-bucket',
            options=[{'label': i, 'value': i} for i in ['2 Years', '5 Years']],
            value='2',
            labelStyle={'display': 'inline-block'}
        )
    ], style={'width': '60%', 'display': 'inline-block'}),
    dcc.Graph(id='age-vs-spend')
])

@app.callback(
    Output('age-vs-spend', 'figure'),
    [Input('dropdown-ucc', 'value')]
)
def update_graph(dropdown_value):
    filtered_pipe = test_pipe[test_pipe['UCC'] == int(dropdown_value)]
    print(filtered_pipe)
    x = filtered_pipe['AGE_REF']
    y = filtered_pipe['AVG_SPEND']

    for i in range(0, 99999999, 1000):
        u_spline = UnivariateSpline(x, y, s=i)
        knot_count = len(u_spline.get_knots())
        print(knot_count)
        if knot_count <= 8:
            break
    # u_spline = UnivariateSpline(x, y, s=10000)
    xs = np.linspace(20, 80, 1000)

    return {
            'data': [
                go.Scatter(
                    name="Dot Plot",
                    x=filtered_pipe['AGE_REF'],
                    y=filtered_pipe['AVG_SPEND'],
                    # text="testtest",
                    mode='markers',
                    opacity=1.0,
                    marker={
                        'size': 5,
                        'line': {'width': 0.5, 'color': 'white'}
                    }
                ),
                go.Scatter(
                    name="Spline",
                    x=xs,
                    y=u_spline(xs),
                    mode="lines",
                    opacity=1.0
                )
            ],
            'layout': go.Layout(
                title=ucc_dict[dropdown_value],
                xaxis={'type': 'linear', 'title': 'Age in Years'},
                yaxis={'title': 'Average Spend in $'},
                margin={'l': 80, 'b': 70, 't': 50, 'r': 20},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }


if __name__ == '__main__':
    app.run_server()
