import numpy as np
import pandas as pd
from scipy.interpolate import UnivariateSpline
from scipy.signal.windows import gaussian
from scipy.ndimage import filters
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import config
import constants
import os
import pickle

app = dash.Dash(__name__)
server = app.server

ucc_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "ucc_data_dictionary.csv"))
ucc_pipe['UCC_DESCRIPTION'].fillna(ucc_pipe['UCC'].astype(str), inplace=True)
ucc_dict = pd.Series(ucc_pipe['UCC_DESCRIPTION'].values, index=ucc_pipe['UCC']).to_dict()

fmli_category_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "fmli_data_dictionary.csv"))
fmli_dict = pd.Series(fmli_category_pipe['CAT_DESCRIPTION'].values, index=fmli_category_pipe['CAT_CODE']).to_dict()

app.layout = html.Div([
    html.Div([
        html.H3('Consumer Expenditure Survey', style={'textAlign': 'center'}),
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
        ], style={'width': '50%', 'display': 'inline-block'}),
        html.Div([
            html.Label('Bucket', style={'fontWeight': '600'}),
            dcc.RadioItems(
                id='years-bucket',
                options=[{'label': i + ' Years', 'value': i} for i in ['3', '5']],
                value='3',
                labelStyle={'marginRight': '20px', 'display': 'inline-block'}
            )
        ], style={'width': '50%', 'float': 'right', 'display': 'inline-block'}),

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


def year_bucket_files(years_bucket):
    years_bucket = int(years_bucket)
    if years_bucket == 3:
        return constants.AVG_SPEND_FILES_3_YEAR
    elif years_bucket == 5:
        return constants.AVG_SPEND_FILES_5_YEAR


def category_dict_for_file(file_type):
    if file_type == 'mtbi':
        return ucc_dict
    elif file_type == 'fmli':
        return fmli_dict


def spline_dict_for_file(file_type, part_file_name):
    file_name = "{}_{}_{}.pkl".format(file_type, 'spline', part_file_name)
    file_path = os.path.join(config.SPLINES_FOLDER_PATH, file_name)

    with open(file_path, 'rb') as file:
        spline_dict = pickle.load(file)

    return spline_dict


@app.callback(
    Output('dropdown-category', 'options'),
    [Input('file-type', 'value')]
)
def update_dropdown_options(file_type):
    category_dict = category_dict_for_file(file_type)
    return [{'label': val, 'value': key} for key, val in category_dict.items()]


@app.callback(
    Output('dropdown-category', 'value'),
    [Input('file-type', 'value')]
)
def update_dropdown_options(file_type):
    category_dict = category_dict_for_file(file_type)
    return list(category_dict.keys())[0]


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
    [Input('dropdown-category', 'value'),
     Input('file-type', 'value'),
     Input('years-bucket', 'value'),
     Input('year-slider', 'value')]
)
def update_graph(category_value, file_type, bucket_value, year_slider_value):
    print("**** Category Value: {} *****".format(category_value))

    slider_label = list(year_bucket_files(bucket_value).keys())[int(year_slider_value)]
    part_file_name = year_bucket_files(bucket_value)[slider_label]
    file_name = "{}_{}.csv".format(file_type, part_file_name)
    print(file_name)
    file_path = os.path.join(config.DATA_FILES_PATH, "processed_data_{}yrs_bucket_jun26".format(int(bucket_value)), file_name)
    avg_spend_pipe = pd.read_csv(file_path)

    if file_type == 'mtbi':
        filtered_pipe = avg_spend_pipe[avg_spend_pipe['UCC'] == int(category_value)]
    elif file_type == 'fmli':
        filtered_pipe = avg_spend_pipe[['AGE_REF', category_value]]

    spline_dict = spline_dict_for_file(file_type, part_file_name)
    u_spline = spline_dict[category_value]

    if filtered_pipe.empty or u_spline is None:
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
    y = filtered_pipe['AVG_SPEND'] if file_type == 'mtbi' else filtered_pipe[category_value]

    xs = np.linspace(20, 80, 1000)

    return {
            'data': [
                go.Scatter(
                    name="Dot Plot",
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
                    name="Spline",
                    x=xs,
                    y=u_spline(xs),
                    mode="lines",
                    opacity=1.0
                )
            ],
            'layout': go.Layout(
                title=category_dict_for_file(file_type)[category_value],
                xaxis={'type': 'linear', 'title': 'Age in Years'},
                yaxis={'title': 'Average Spend in $'},
                margin={'l': 80, 'b': 70, 't': 50, 'r': 20},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }


if __name__ == '__main__':
    app.run_server(debug=True)
