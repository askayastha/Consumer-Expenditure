import numpy as np
import pandas as pd
from scipy.interpolate import UnivariateSpline
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import config
import constants
import os

app = dash.Dash(__name__)
server = app.server

# test_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "processed_data_{}yrs_bucket_jun26/mtbi_avg_spend_intrvw_2011_to_2015.csv".format(5)))
ucc_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "ucc_data_dictionary.csv"))

ucc_pipe['UCC_DESCRIPTION'].fillna(ucc_pipe['UCC'].astype(str), inplace=True)
ucc_dict = pd.Series(ucc_pipe['UCC_DESCRIPTION'].values, index=ucc_pipe['UCC']).to_dict()

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
        html.H1('Consumer Expenditure Survey'),
        html.Label('Expense Category'),
        dcc.Dropdown(
            id='dropdown-ucc',
            options=[{'label': val, 'value': key} for key, val in ucc_dict.items()],
            value=ucc_pipe['UCC'][0]
        ),
        html.Br(),
        html.Label('Bucket'),
        dcc.RadioItems(
            id='years-bucket',
            options=[{'label': i + ' Years', 'value': i} for i in ['3', '5']],
            value='3',
            labelStyle={'display': 'inline-block'}
        )
    ], style={'width': '100%', 'display': 'inline-block'}),
    dcc.Graph(id='age-vs-spend'),
    html.Div([
        dcc.Slider(
            id='year-slider',
            min=0,
            step=None
        )
    ], style={'margin-left': '10%', 'margin-right': '10%'})

])


def year_bucket_files(years_bucket):
    years_bucket = int(years_bucket)
    if years_bucket == 3:
        return constants.MTBI_FILES_3_YEAR
    elif years_bucket == 5:
        return constants.MTBI_FILES_5_YEAR


@app.callback(
    Output('year-slider', 'max'),
    [Input('years-bucket', 'value')]
)
def update_slider_max(bucket_value):
    return len(year_bucket_files(bucket_value)) - 1


@app.callback(
    Output('year-slider', 'value'),
    [Input('years-bucket', 'value')]
)
def update_slider_value(bucket_value):
    return len(year_bucket_files(bucket_value)) - 1


@app.callback(
    Output('year-slider', 'marks'),
    [Input('years-bucket', 'value')]
)
def update_slider_marks(bucket_value):
    return {
        i: list(year_bucket_files(bucket_value).keys())[i] for i in range(len(year_bucket_files(bucket_value)))
    }


@app.callback(
    Output('age-vs-spend', 'figure'),
    [Input('dropdown-ucc', 'value'),
     Input('years-bucket', 'value'),
     Input('year-slider', 'value')]
)
def update_graph(ucc_value, bucket_value, year_slider_value):
    print("**** UCC Value: {} *****".format(ucc_value))

    slider_label = list(year_bucket_files(bucket_value).keys())[int(year_slider_value)]
    file_name = year_bucket_files(bucket_value)[slider_label]
    print(file_name)
    avg_spend_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH,
                                         "processed_data_{}yrs_bucket_jun26/{}.csv".format(
                                             int(bucket_value), file_name)))

    filtered_pipe = avg_spend_pipe[avg_spend_pipe['UCC'] == int(ucc_value)]
    if filtered_pipe.empty:
        print("***** NO DATA *****")
        return {
            'data': [],
            'layout': go.Layout(
                title='NO DATA!',
                xaxis={'type': 'linear', 'title': 'Age in Years'},
                yaxis={'title': 'Average Spend in $'},
                margin={'l': 80, 'b': 70, 't': 50, 'r': 20},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
    # print(filtered_pipe)
    x = filtered_pipe['AGE_REF']
    y = filtered_pipe['AVG_SPEND']

    for i in range(0, 99999999, 10000):
        u_spline = UnivariateSpline(x, y, s=i)
        knot_count = len(u_spline.get_knots())
        # print(knot_count)
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
                title=ucc_dict[ucc_value],
                xaxis={'type': 'linear', 'title': 'Age in Years'},
                yaxis={'title': 'Average Spend in $'},
                margin={'l': 80, 'b': 70, 't': 50, 'r': 20},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }


if __name__ == '__main__':
    app.run_server(debug=True)
