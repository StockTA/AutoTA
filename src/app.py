#!/usr/bin/env python
# coding: utf-8

# In[48]:

# Imports
from dash import Dash, dash_table, dcc, html, Input, Output, callback
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# Database connection
# for postgreSQL database credentials can be written as
user = 'autotadb_user'
password = 'yYTTKe6G7OMZQUxvh0POHAoo4dEeSKdA'
host = 'dpg-ctdqq3jv2p9s73c8jk60-a.virginia-postgres.render.com'
port = '5432'
database = 'autotadb'
# for creating connection string
connection_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
#connection_str = "postgresql://autotadb_user:yYTTKe6G7OMZQUxvh0POHAoo4dEeSKdA@dpg-ctdqq3jv2p9s73c8jk60-a.virginia-postgres.render.com/autotadb"

# SQLAlchemy engine
engine = create_engine(connection_str)
with engine.connect() as connection_str:
    dbConnection = engine.connect();

#database = "./AutoTA.db"
#connection = sql.connect(database)
#query1 = '''SELECT * FROM Portfolio_Score'''
#query2 = '''SELECT * FROM AssetClass_Score'''
#query3 = '''SELECT * FROM ETF_Score'''
#query4 = '''SELECT * FROM Stock_Score'''
#df1 = pd.read_sql_query(query1, dbConnection)

df1 = pd.read_sql_table('Portfolio_Score', dbConnection, schema='public')
df2 = pd.read_sql_table('AssetClass_Score', dbConnection, schema='public')
df3 = pd.read_sql_table('ETF_Score', dbConnection, schema='public')
df4 = pd.read_sql_table('Stock_Score', dbConnection, schema='public')
df1.fillna(0)
df2.fillna(0)
df3.fillna(0)
df4.fillna(0)
df = df1

app = Dash(__name__)
server = app.server

app.layout = html.Div([

    html.H1('AutoTA'),

    html.Div([ 
        dcc.Dropdown(
	    id='dataframe_dropdown',
            options=[
	        {'label': 'Portfolio', 'value': 1},
	        {'label': 'Asset Classes', 'value': 2},
	        {'label': 'ETFs', 'value': 3},
	        {'label': 'Stocks', 'value': 4}
            ],
            optionHeight=25,
            value=1,
            multi=False,
            searchable=False,
            search_value='',
            placeholder='Please select tickers',
            clearable=False,
            style={'width':'75%'},
        ),
        html.Div(id='datatable-container') 
    ]),

    html.H2('Bullish Trending', style={"color": "green"}),

    html.Div([
        dash_table.DataTable(
            id='datatable_1',
            data=df.to_dict('records'),  # not can't filter her for trade, it will be overwritted by dropdown
            #columns=[
            #    {'id': c, 'name': c} for c in df.columns],
            columns=[
                {"name": "Sym", "id": "Ticker"},
                {"name": "Desc", "id": "Description"},
                {"name": "Class", "id": "Class"},
                {"name": "Tr", "id": "Trade"},
                {"name": "Scr", "id": "Score"},
                {"name": "Chg", "id": "Change1D"},
                {"name": "ZScore", "id": "Reg3MZScore"},
                {"name": "ATRx", "id": "ATRx"},
                {"name": "Exp", "id": "Explanation"}],
            #editable = False,
            #filter_action="native"
            #sort_action="native"
            #row_selection=False,
            #row_deletable=False,
            #selected_rows=[],
            #page_action=none,
            #page_current=0,
            #page_size=6,
            style_cell={
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
                'padding': '0px',
                'padding-left': '3px'
                },
            tooltip_data=[
                {
                column: {'value': str(value), 'type': 'markdown'} 
                for column, value in row.items() if column in ["Explanation"]
                } for row in df.to_dict('records')
            ],
            tooltip_delay=0,
            tooltip_duration=None,
            css=[{
                'selector': '.dash-table-tooltip',
                'rule': 'background-color: grey; font-family: monospace; color: white; padding: 0px'
            }],
            fixed_rows={'headers': True, 'data': 0},
            virtualization=False,
            style_header={
                'backgroundColor': 'grey',
                'fontWeight': 'bold'},
            style_cell_conditional=[
                {'if': {'column_id': 'Ticker'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Description'},
                 'width': '10%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Class'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Trade'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Score'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Change1D'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Reg3MZScore'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'ATRx'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Explanation'},
                 'width': '5%',
                 'textAlign': 'left'},
            ],
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)'},
            ],
        ),
    ]), 

    html.H2('Bullish Mean Reverting', style={"color": "green"}),

    html.Div([
        dash_table.DataTable(
            id='datatable_2',
            data=df.to_dict('records'),  # not can't filter her for trade, it will be overwritted by dropdown
            columns=[
                {"name": "Sym", "id": "Ticker"},
                {"name": "Desc", "id": "Description"},
                {"name": "Class", "id": "Class"},
                {"name": "Tr", "id": "Trade"},
                {"name": "Scr", "id": "Score"},
                {"name": "Chg", "id": "Change1D"},
                {"name": "ZScore", "id": "Reg3MZScore"},
                {"name": "ATRx", "id": "ATRx"},
                {"name": "Exp", "id": "Explanation"}],
            #editable = False,
            #filter_action="native"
            #sort_action="native"
            #row_selection=False,
            #row_deletable=False,
            #selected_rows=[],
            #page_action=none,
            #page_current=0,
            #page_size=6,
            style_cell={
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
                'padding': '0px',
                'padding-left': '3px'
                },
            tooltip_data=[
                {
                column: {'value': str(value), 'type': 'markdown'} 
                for column, value in row.items() if column in ["Explanation"]
                } for row in df.to_dict('records')
            ],
            tooltip_delay=0,
            tooltip_duration=None,
            css=[{
                'selector': '.dash-table-tooltip',
                'rule': 'background-color: grey; font-family: monospace; color: white; padding: 0px'
            }],
            fixed_rows={'headers': True, 'data': 0},
            virtualization=False,
            style_header={
                'backgroundColor': 'grey',
                'fontWeight': 'bold'},
            style_cell_conditional=[
                {'if': {'column_id': 'Ticker'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Description'},
                 'width': '10%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Class'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Trade'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Score'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Change1D'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Reg3MZScore'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'ATRx'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Explanation'},
                 'width': '5%',
                 'textAlign': 'left'},
            ],
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)'},
            ],
        ),
    ]), 

    html.H2('Bearish Trending', style={"color": "red"}),

    html.Div([
        dash_table.DataTable(
            id='datatable_3',
            data=df.to_dict('records'),  # not can't filter her for trade, it will be overwritted by dropdown
            columns=[
                {"name": "Sym", "id": "Ticker"},
                {"name": "Desc", "id": "Description"},
                {"name": "Class", "id": "Class"},
                {"name": "Tr", "id": "Trade"},
                {"name": "Scr", "id": "Score"},
                {"name": "Chg", "id": "Change1D"},
                {"name": "ZScore", "id": "Reg3MZScore"},
                {"name": "ATRx", "id": "ATRx"},
                {"name": "Exp", "id": "Explanation"}],
            #editable = False,
            #filter_action="native"
            #sort_action="native"
            #row_selection=False,
            #row_deletable=False,
            #selected_rows=[],
            #page_action=none,
            #page_current=0,
            #page_size=6,
            style_cell={
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
                'padding': '0px',
                'padding-left': '3px'
                },
            tooltip_data=[
                {
                column: {'value': str(value), 'type': 'markdown'} 
                for column, value in row.items() if column in ["Explanation"]
                } for row in df.to_dict('records')
            ],
            tooltip_delay=0,
            tooltip_duration=None,
            css=[{
                'selector': '.dash-table-tooltip',
                'rule': 'background-color: grey; font-family: monospace; color: white; padding: 0px'
            }],
            fixed_rows={'headers': True, 'data': 0},
            virtualization=False,
            style_header={
                'backgroundColor': 'grey',
                'fontWeight': 'bold'},
            style_cell_conditional=[
                {'if': {'column_id': 'Ticker'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Description'},
                 'width': '10%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Class'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Trade'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Score'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Change1D'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Reg3MZScore'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'ATRx'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Explanation'},
                 'width': '5%',
                 'textAlign': 'left'},
            ],
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)'},
            ],
        ),
    ]), 

    html.H2('Bearish Mean Reverting', style={"color": "red"}),

    html.Div([
        dash_table.DataTable(
            id='datatable_4',
            data=df.to_dict('records'),  # not can't filter her for trade, it will be overwritted by dropdown
            columns=[
                {"name": "Sym", "id": "Ticker"},
                {"name": "Desc", "id": "Description"},
                {"name": "Class", "id": "Class"},
                {"name": "Tr", "id": "Trade"},
                {"name": "Scr", "id": "Score"},
                {"name": "Chg", "id": "Change1D"},
                {"name": "ZScore", "id": "Reg3MZScore"},
                {"name": "ATRx", "id": "ATRx"},
                {"name": "Exp", "id": "Explanation"}],
            #editable = False,
            #filter_action="native"
            #sort_action="native"
            #row_selection=False,
            #row_deletable=False,
            #selected_rows=[],
            #page_action=none,
            #page_current=0,
            #page_size=6,
            style_cell={
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
                'padding': '0px',
                'padding-left': '3px'
                },
            tooltip_data=[
                {
                column: {'value': str(value), 'type': 'markdown'} 
                for column, value in row.items() if column in ["Explanation"]
                } for row in df.to_dict('records')
            ],
            tooltip_delay=0,
            tooltip_duration=None,
            css=[{
                'selector': '.dash-table-tooltip',
                'rule': 'background-color: grey; font-family: monospace; color: white; padding: 0px'
            }],
            fixed_rows={'headers': True, 'data': 0},
            virtualization=False,
            style_header={
                'backgroundColor': 'grey',
                'fontWeight': 'bold'},
            style_cell_conditional=[
                {'if': {'column_id': 'Ticker'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Description'},
                 'width': '10%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Class'},
                 'width': '5%',
                 'textAlign': 'left'},
                {'if': {'column_id': 'Trade'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Score'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Change1D'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Reg3MZScore'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'ATRx'},
                 'width': '5%',
                 'textAlign': 'center'},
                {'if': {'column_id': 'Explanation'},
                 'width': '5%',
                 'textAlign': 'left'},
            ],
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)'},
            ],
        ),
    ]), 

])


@app.callback(
    Output('datatable_1', 'data'),
    Output('datatable_2', 'data'),
    Output('datatable_3', 'data'),
    Output('datatable_4', 'data'),
    Output('datatable_1', 'tooltip_data'),
    Output('datatable_2', 'tooltip_data'),
    Output('datatable_3', 'tooltip_data'),
    Output('datatable_4', 'tooltip_data'),
    Input('dataframe_dropdown', 'value')
)
def update_datatable(value):
    if value == 1:
        dfTFBull = df1[(df1['Trade'] == 'TF') & (df1['Score'] > 0)]
        dfMRBull = df1[(df1['Trade'] == 'MR') & (df1['Score'] > 0)]
        dfTFBear = df1[(df1['Trade'] == 'TF') & (df1['Score'] < 0)]
        dfMRBear = df1[(df1['Trade'] == 'MR') & (df1['Score'] < 0)]
        dfTFBull = dfTFBull.sort_values(by='Score', ascending=False)
        dfMRBull = dfMRBull.sort_values(by='Score', ascending=False)
        dfTFBear = dfTFBear.sort_values(by='Score', ascending=True)
        dfMRBear = dfMRBear.sort_values(by='Score', ascending=True)
        dataTFBull = dfTFBull.to_dict('records')
        dataMRBull = dfMRBull.to_dict('records')
        dataTFBear = dfTFBear.to_dict('records')
        dataMRBear = dfMRBear.to_dict('records')
        tooltipTFBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBull.to_dict('records')]
        tooltipMRBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBull.to_dict('records')]
        tooltipTFBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBear.to_dict('records')]
        tooltipMRBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBear.to_dict('records')]
        #tooltip_data= [{c:{'type': 'text', 'value': f'{r},{c}'} for c in df.columns} for r in df[df.columns].values]
        return dataTFBull, dataMRBull, dataTFBear, dataMRBear, tooltipTFBull, tooltipMRBull, tooltipTFBear, tooltipTFBear
    if value == 2:
        dfTFBull = df2[(df2['Trade'] == 'TF') & (df2['Score'] > 0)]
        dfMRBull = df2[(df2['Trade'] == 'MR') & (df2['Score'] > 0)]
        dfTFBear = df2[(df2['Trade'] == 'TF') & (df2['Score'] < 0)]
        dfMRBear = df2[(df2['Trade'] == 'MR') & (df2['Score'] < 0)]
        dfTFBull = dfTFBull.sort_values(by='Score', ascending=False)
        dfMRBull = dfMRBull.sort_values(by='Score', ascending=False)
        dfTFBear = dfTFBear.sort_values(by='Score', ascending=True)
        dfMRBear = dfMRBear.sort_values(by='Score', ascending=True)
        dataTFBull = dfTFBull.to_dict('records')
        dataMRBull = dfMRBull.to_dict('records')
        dataTFBear = dfTFBear.to_dict('records')
        dataMRBear = dfMRBear.to_dict('records')
        tooltipTFBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBull.to_dict('records')]
        tooltipMRBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBull.to_dict('records')]
        tooltipTFBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBear.to_dict('records')]
        tooltipMRBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBear.to_dict('records')]
        return dataTFBull, dataMRBull, dataTFBear, dataMRBear, tooltipTFBull, tooltipMRBull, tooltipTFBear, tooltipTFBear
    if value == 3:
        dfTFBull = df3[(df3['Trade'] == 'TF') & (df3['Score'] > 0)]
        dfMRBull = df3[(df3['Trade'] == 'MR') & (df3['Score'] > 0)]
        dfTFBear = df3[(df3['Trade'] == 'TF') & (df3['Score'] < 0)]
        dfMRBear = df3[(df3['Trade'] == 'MR') & (df3['Score'] < 0)]
        dfTFBull = dfTFBull.sort_values(by='Score', ascending=False)
        dfMRBull = dfMRBull.sort_values(by='Score', ascending=False)
        dfTFBear = dfTFBear.sort_values(by='Score', ascending=True)
        dfMRBear = dfMRBear.sort_values(by='Score', ascending=True)
        dataTFBull = dfTFBull.to_dict('records')
        dataMRBull = dfMRBull.to_dict('records')
        dataTFBear = dfTFBear.to_dict('records')
        dataMRBear = dfMRBear.to_dict('records')
        tooltipTFBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBull.to_dict('records')]
        tooltipMRBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBull.to_dict('records')]
        tooltipTFBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBear.to_dict('records')]
        tooltipMRBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBear.to_dict('records')]
        return dataTFBull, dataMRBull, dataTFBear, dataMRBear, tooltipTFBull, tooltipMRBull, tooltipTFBear, tooltipTFBear
    if value == 4:
        dfTFBull = df4[(df4['Trade'] == 'TF') & (df4['Score'] > 0)]
        dfMRBull = df4[(df4['Trade'] == 'MR') & (df4['Score'] > 0)]
        dfTFBear = df4[(df4['Trade'] == 'TF') & (df4['Score'] < 0)]
        dfMRBear = df4[(df4['Trade'] == 'MR') & (df4['Score'] < 0)]
        dfTFBull = dfTFBull.sort_values(by='Score', ascending=False)
        dfMRBull = dfMRBull.sort_values(by='Score', ascending=False)
        dfTFBear = dfTFBear.sort_values(by='Score', ascending=True)
        dfMRBear = dfMRBear.sort_values(by='Score', ascending=True)
        dataTFBull = dfTFBull.to_dict('records')
        dataMRBull = dfMRBull.to_dict('records')
        dataTFBear = dfTFBear.to_dict('records')
        dataMRBear = dfMRBear.to_dict('records')
        tooltipTFBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBull.to_dict('records')]
        tooltipMRBull=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBull.to_dict('records')]
        tooltipTFBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfTFBear.to_dict('records')]
        tooltipMRBear=[{column: {'value': str(value), 'type': 'markdown'} 
            for column, value in row.items() if column in ["Explanation"]} for row in dfMRBear.to_dict('records')]
        return dataTFBull, dataMRBull, dataTFBear, dataMRBear, tooltipTFBull, tooltipMRBull, tooltipTFBear, tooltipTFBear
    #raise PreventUpdate
    #return no_update, no_update, no_update, no_update

if __name__ == '__main__':
    app.run(debug=True)


# In[ ]:


