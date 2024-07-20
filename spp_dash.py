# %%
import bp_sql as bp
import pandas as pd
import numpy as np

import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
# %%
tgt_db = 'SPP.db'

conn = bp.create_connection(db_name=tgt_db)

df = pd.read_sql_query('''Select*
                          FROM ercot_avg_spp
                       where settlement_point_name in ('LZ_NORTH','LZ_WEST','LZ_HOUSTON','LZ_SOUTH')
                       ''', con=conn)
df['DELIVERY_DATE'] = pd.to_datetime(df['DELIVERY_DATE'])
df['DELIVERY_HOUR'] = df['DELIVERY_HOUR'].astype(int)
df['dt']= df['DELIVERY_DATE'] + pd.to_timedelta(df['DELIVERY_HOUR']-1,unit='h')
df['year'] = df.DELIVERY_DATE.dt.year

# Color mapping dictionary for specified SETTLEMENT_POINT_NAMEs
color_mapping = {
    'LZ_HOUSTON': 'orange',
    'LZ_NORTH': 'blue',
    'LZ_WEST': 'red',
    'LZ_SOUTH': 'green'
}
# %%
# Get unique years in the dataset
unique_years = df['year'].unique()

# Initialize the Dash app
app = dash.Dash(__name__)

# Create a dropdown menu to select the year
dropdown = dcc.Dropdown(
    id='year-dropdown',
    options=[{'label': str(year), 'value': year} for year in unique_years],
    value=unique_years[0]  # Set default value to the first year
)

# Define the layout of the app
app.layout = html.Div([
    dropdown,
    dcc.Graph(id='graph')
])

# Define callback to update the graph based on the selected year
@app.callback(
    Output('graph', 'figure'),
    [Input('year-dropdown', 'value')]
)
def update_graph(selected_year):
    year_data = df[df['year'] == selected_year]
    fig = px.line(year_data, x='dt', y='SETTLEMENT_POINT_PRICE', color='SETTLEMENT_POINT_NAME',
                  title=f'Energy Prices for Year {selected_year}')
    
    for settlement, color in color_mapping.items():
        fig.for_each_trace(lambda trace: trace.update(line=dict(color=color)) 
                           if trace.name == settlement else (), selector={})
    
    fig.update_xaxes(
        dtick="M1",  # Show every month
        tickformat="%b"  # Month abbreviation
    )
    
    fig.update_layout(
        xaxis_title='Month',
        yaxis_title='SETTLEMENT_POINT_PRICE',
        yaxis=dict(range=[0, 9000])  # Set y-axis range to 0-9000
    )
    
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
# %%
