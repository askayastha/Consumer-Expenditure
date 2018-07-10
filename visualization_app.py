import numpy as np
import pandas as pd
from scipy.interpolate import UnivariateSpline
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import config
import os
import utils

app = dash.Dash(__name__)
server = app.server

ucc_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "ucc_data_dictionary.csv"))
ucc_pipe['UCC_DESCRIPTION'].fillna(ucc_pipe['UCC'].astype(str), inplace=True)
ucc_dict = pd.Series(ucc_pipe['UCC_DESCRIPTION'].values, index=ucc_pipe['UCC']).to_dict()

fmli_category_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "fmli_data_dictionary.csv"))
fmli_dict = pd.Series(fmli_category_pipe['CAT_DESCRIPTION'].values, index=fmli_category_pipe['CAT_CODE']).to_dict()

app.layout = html.Div([
    html.Div([
        html.H3('Consumer Expenditure Survey', style={'textAlign': 'center', 'fontWeight': '600'}),
        html.Label('Expense Category', style={'fontWeight': '600'}),
        dcc.Dropdown(
            id='dropdown-category'
        ),
        html.Br(),
        html.Div([
            html.Label('File Type', style={'fontWeight': '600'}),
            dcc.RadioItems(
                id='file-type',
                options=[{'label': i, 'value': i.lower()} for i in ['MTBI', 'FMLI']],
                value='mtbi',
                labelStyle={'marginRight': '20px', 'display': 'inline-block'}
            )
        ], style={'width': '33%', 'display': 'inline-block'}),
        html.Div([
            html.Label('Bucket Size', style={'fontWeight': '600'}),
            dcc.RadioItems(
                id='bucket-size',
                options=[{'label': i + ' Years', 'value': i} for i in ['3']],
                value='3',
                labelStyle={'marginRight': '20px', 'display': 'inline-block'}
            )
        ], style={'width': '33%', 'display': 'inline-block'}),
        html.Div([
            html.Label('Graph', style={'fontWeight': '600'}),
            dcc.RadioItems(
                id='graph-type',
                options=[{'label': i, 'value': i.lower().replace(' ', '-')} for i in ['Individual Bucket', 'All Buckets']],
                value='individual-bucket',
                labelStyle={'marginRight': '20px', 'display': 'inline-block'}
            )
        ], style={'width': '33%', 'float': 'right', 'display': 'inline-block'})

    ], style={'width': '100%', 'display': 'inline-block'}),
    html.Br(),
    html.Br(),
    dcc.Graph(id='age-vs-spend'),
    html.Div([
        dcc.Slider(
            id='year-slider',
            min=0,
            step=None
        )
    ], style={'margin-left': '10%', 'margin-right': '10%'})

])


app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})


@app.callback(
    Output('dropdown-category', 'options'),
    [Input('file-type', 'value')]
)
def update_dropdown_options(file_type):
    category_dict = utils.category_dict_for_file(file_type)
    return [{'label': val, 'value': key} for key, val in category_dict.items()]


@app.callback(
    Output('dropdown-category', 'value'),
    [Input('file-type', 'value')]
)
def update_dropdown_options(file_type):
    category_dict = utils.category_dict_for_file(file_type)
    return list(category_dict.keys())[0]


@app.callback(
    Output('year-slider', 'max'),
    [Input('bucket-size', 'value')]
)
def update_slider_max(bucket_size):
    return len(utils.avg_spend_files_for_bucket(bucket_size)) - 1


@app.callback(
    Output('year-slider', 'value'),
    [Input('bucket-size', 'value')]
)
def update_slider_value(bucket_size):
    return len(utils.avg_spend_files_for_bucket(bucket_size)) - 1


@app.callback(
    Output('year-slider', 'marks'),
    [Input('bucket-size', 'value')]
)
def update_slider_marks(bucket_size):
    return {
        i: list(utils.avg_spend_files_for_bucket(bucket_size).keys())[i] for i in range(len(utils.avg_spend_files_for_bucket(bucket_size)))
    }


@app.callback(
    Output('graph-type', 'value'),
    [Input('year-slider', 'value')]
)
def update_graph_type_value(year_slider):
    return 'individual-bucket'


@app.callback(
    Output('age-vs-spend', 'figure'),
    [Input('dropdown-category', 'value'),
     Input('file-type', 'value'),
     Input('bucket-size', 'value'),
     Input('graph-type', 'value'),
     Input('year-slider', 'value')]
)
def update_graph(category_value, file_type, bucket_size, graph_type, year_slider_value):
    print("**** Category Value: {} *****".format(category_value))

    slider_label = list(utils.avg_spend_files_for_bucket(bucket_size).keys())[int(year_slider_value)]
    part_file_name = utils.avg_spend_files_for_bucket(bucket_size)[slider_label]
    file_name = "{}_{}.csv".format(file_type, part_file_name)
    print(file_name)
    file_path = os.path.join(config.DATA_FILES_PATH, "processed_data_{}yrs_bucket_jun26".format(int(bucket_size)), file_name)
    avg_spend_pipe = pd.read_csv(file_path)

    if file_type == 'mtbi':
        filtered_pipe = avg_spend_pipe[avg_spend_pipe['UCC'] == int(category_value)]
    elif file_type == 'fmli':
        filtered_pipe = avg_spend_pipe[['AGE_REF', category_value]]

    spline_dict = utils.spline_dict_for_file(file_type, part_file_name)
    u_spline = spline_dict[category_value]

    if graph_type == 'individual-bucket' and (filtered_pipe.empty or u_spline is None):
        print("***** NO DATA *****")
        return {
            'data': [],
            'layout': go.Layout(
                title='NO DATA!',
                xaxis={'type': 'linear', 'title': 'Age (Years)'},
                yaxis={'title': 'Average ($)'},
                margin={'l': 80, 'b': 70, 't': 50, 'r': 20},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
    # print(filtered_pipe)
    x = filtered_pipe['AGE_REF']
    y = filtered_pipe['AVG_SPEND'] if file_type == 'mtbi' else filtered_pipe[category_value]

    # _, var = process_splines.moving_average(y)
    # u_spline = UnivariateSpline(x, y, w=1 / np.sqrt(var))
    xs = np.linspace(20, 80, 1000)

    if graph_type == 'individual-bucket':
        return {
            'data': [
                go.Scatter(
                    name="Scatter Plot",
                    x=x,
                    y=y,
                    # text="testtest",
                    mode='markers',
                    opacity=1.0,
                    marker={
                        'size': 5,
                        'line': {'width': 0.5, 'color': 'white'}
                    }
                ),
                go.Scatter(
                    name=slider_label,
                    x=xs,
                    y=u_spline(xs),
                    mode="lines",
                    opacity=1.0
                )
            ],
            'layout': go.Layout(
                title=utils.category_dict_for_file(file_type)[category_value],
                xaxis={'type': 'linear', 'title': 'Age (Years)'},
                yaxis={'title': 'Average ($)'},
                margin={'l': 80, 'b': 70, 't': 50, 'r': 20},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }

    elif graph_type == 'all-buckets':
        traces = []
        for spline_label, part_file_name in utils.avg_spend_files_for_bucket(bucket_size).items():
            spline_dict = utils.spline_dict_for_file(file_type, part_file_name)

            if spline_dict[category_value] is not None:
                traces.append(
                    go.Scatter(
                        name=spline_label,
                        x=xs,
                        y=spline_dict[category_value](xs),
                        mode="lines",
                        opacity=1.0
                        # line={'width': 1}
                    )
                )

        layout = go.Layout(
            title=utils.category_dict_for_file(file_type)[category_value],
            xaxis={'type': 'linear', 'title': 'Age (Years)'},
            yaxis={'title': 'Average ($)'},
            margin={'l': 80, 'b': 70, 't': 50, 'r': 20},
            legend={'x': 0, 'y': 1},
            hovermode='closest'
        )

        return {'data': traces, 'layout': layout}


if __name__ == '__main__':
    app.run_server(debug=True)
