import dash_bootstrap_components as dbc
import psycopg2
from dash import Dash, dash_table, html, dcc, Input, Output
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


conn = psycopg2.connect(dbname='postgres', user='postgres',
                        password='', host='127.0.0.1', port='5432')


app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


df = pd.read_sql("select * from musique limit 10", conn)


# Traitement des données dans un premier temps en mettant la colonne des dates sous le même format
df['date'] = df['date'].astype('datetime64[ns]')
df['date_modified'] = df['date'].dt.strftime('%m/%d/%Y')
df = df.drop(columns=['date'])
df['date_modified'] = df['date_modified'].astype('datetime64[ns]')


df_head = df.head(15)

# requetes sql pour obtenir le dashboard

df_q2 = pd.read_sql(
    "select song, count(artist) as numberOfSongListened from musique group by artist, song order by (numberOfSongListened) desc limit 1;", conn)
value_df2 = int(df_q2['numberofsonglistened'])

print(df_q2.columns)

df_q3 = pd.read_sql(
    "select album, count(album) as numberAlbumListened from musique group by album order by (numberAlbumListened) desc limit 1;", conn)
value_df3 = int(df_q3['numberalbumlistened'])

df_q4 = pd.read_sql(
    "select artist, count(artist) as numberArtistListened from musique group by artist order by (numberArtistListened) desc limit 10;", conn
)

df_q5 = pd.read_sql(
    """SELECT *
FROM (
    SELECT t1.Week, t1.artist, COUNT(*) as count, t1.year,
           ROW_NUMBER() OVER (PARTITION BY t1.Week, t1.year ORDER BY COUNT(*) DESC) as rank
    FROM (
        SELECT date_part('week', date) as Week, artist, date_part('year', date) as year
        FROM musique
    ) as t1
    JOIN (
        SELECT date_part('week', date) as Week, artist, date_part('year', date) as year
        FROM musique
        GROUP BY date_part('week', date), artist, date_part('year', date) 
    ) as t2
    ON t1.Week = t2.Week AND t1.artist = t2.artist
    GROUP BY t1.Week, t1.artist, t1.year
) as t
WHERE rank <= 10
ORDER BY year, Week ASC, count DESC""", conn
)

df_q6 = pd.read_sql("""
SELECT *
FROM (
    SELECT t1.Week, t1.song, COUNT(*) as count, t1.year,
           ROW_NUMBER() OVER (PARTITION BY t1.Week, t1.year ORDER BY COUNT(*) DESC) as rank
    FROM (
        SELECT date_part('week', date) as Week, song, date_part('year', date) as year
        FROM musique
    ) as t1
    JOIN (
        SELECT date_part('week', date) as Week, song, date_part('year', date) as year
        FROM musique
        GROUP BY date_part('week', date), song, date_part('year', date) 
    ) as t2
    ON t1.Week = t2.Week AND t1.song= t2.song
    GROUP BY t1.Week, t1.song, t1.year
) as t
WHERE rank <= 1
ORDER BY year, Week ASC, count DESC
""", conn)


df_q7 = pd.read_sql("""
SELECT *
FROM (
    SELECT t1.Week, t1.album, COUNT(*) as count, t1.year,
           ROW_NUMBER() OVER (PARTITION BY t1.Week, t1.year ORDER BY COUNT(*) DESC) as rank
    FROM (
        SELECT date_part('week', date) as Week, album, date_part('year', date) as year
        FROM musique
    ) as t1
    JOIN (
        SELECT date_part('week', date) as Week, album, date_part('year', date) as year
        FROM musique
        GROUP BY date_part('week', date), album, date_part('year', date) 
    ) as t2
    ON t1.Week = t2.Week AND t1.album= t2.album
    GROUP BY t1.Week, t1.album, t1.year
) as t
WHERE rank <= 1
ORDER BY year, Week ASC, count DESC
""", conn)


# figure pour le dashboard
fig = go.Figure(go.Indicator(
    mode="number",
    value=value_df2,
    title={'text': df_q2['song'][0]},
))

fig2 = go.Figure(go.Indicator(
    mode="number",
    value=value_df3,
    title={'text': df_q3['album'][0]},
))

barChart = px.bar(df_q4, x='artist', y='numberartistlistened')
barChart.update_layout(xaxis_title='artists', yaxis_title='Nombre decoute')


# utilisation de la bibliotheque bootstrap pour avoir un affichage harmonieux
card_content = [
    dbc.CardBody(
        [
            html.H5("Card title", className="card-title"),
            html.P(
                "This is some card content that we'll reuse",
                className="card-text",
            ),
        ]
    ),
]

row_0 = dbc.Row(
    [
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5('Some row of the dataset',
                    className="card-title"),
            html.P(
                dbc.Table.from_dataframe(
                    df_head, striped=True, bordered=True, hover=True),
            style={
                'text-align': 'center',
            }),
        ]), color="warning", outline=True), class_name='col-12'),
    ],
    className="mb-4",
)

row_1 = dbc.Row(
    [
        dbc.Col(dbc.Card([
            dbc.CardBody(
                [
                    html.H5('Most listened track all time',
                            className="card-title"),
                    html.P(
                        dcc.Graph(figure=fig, style={
                            'width': '100%',
                            'height': '300px',
                            'margin-left': 'auto',
                            'margin-right': 'auto'}),
                        className="card-text",
                    ),
                ]
            ),
        ], color="primary", outline=True), class_name='col-6'),
        dbc.Col(dbc.Card([
            dbc.CardBody(
                [
                    html.H5('Most listened album all time',
                            className="card-title"),
                    html.P(
                        dcc.Graph(figure=fig2, style={
                            'width': '100%',
                            'height': '300px',
                            'margin-left': 'auto',
                            'margin-right': 'auto'}),
                        className="card-text",
                    ),
                ]
            ),
        ], color="primary", outline=True), class_name='col-6'),
    ],
    className="mb-4",
)

row_2 = dbc.Row(
    [
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5('Ranking of 10 biggest listeners all time',
                    className="card-title"),
            html.P(
                dcc.Graph(figure=barChart, style={
                    'width': '70%',
                    'margin-left': 'auto',
                    'margin-right': 'auto'})),
        ]), color="secondary", outline=True), class_name='col-12'),
    ],
    className="mb-4",
)

row_3 = dbc.Row(
    [
        dbc.Col(dbc.Card(dbc.CardBody(
            [
                html.H5('Ranking of 10 biggest listeners for each week ',
                        className="card-title"),
                html.P(
                    dcc.Graph(id="graph", style={
                        'width': '100%',
                        'height': '500px',
                        'margin-left': 'auto',
                        'margin-right': 'auto'}),
                    className="card-text",
                ),
                dcc.Dropdown(id='year',
                             options=list(df_q5['year'].unique()),
                             value='year', clearable=False,),
                dcc.Dropdown(id='week',
                             options=list(df_q5['week'].unique()),
                             value='week', clearable=False,),
            ]), color="success", outline=True), class_name='col-6'),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5('Ranking of 10 biggest listeners all time ',
                    className="card-title"),
            html.P(
                dcc.Graph(id="graph2", style={
                    'width': '100%',
                    'height': '500px',
                    'margin-left': 'auto',
                    'margin-right': 'auto'}),
                className="card-text",
            ),
            dcc.Dropdown(id='year2',
                         options=list(df_q6['year'].unique()),
                         value='year', clearable=False,),
            dcc.Dropdown(id='week2',
                         options=list(df_q6['week'].unique()),
                         value='week', clearable=False,),
        ]), color="success",
            outline=True), class_name='col-6'),

    ],
    className="mb-4",
)

row_4 = dbc.Row(
    [
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5('Most listened track for each week',
                    className="card-title"),
            html.P(
                dcc.Graph(id="graph3", style={
                    'width': '100%',
                    'height': '500px',
                    'margin-left': 'auto',
                    'margin-right': 'auto'}),
                className="card-text",
            ),
            dcc.Dropdown(id='year3',
                         options=list(df_q7['year'].unique()),
                         value='year', clearable=False,),
            dcc.Dropdown(id='week3',
                         options=list(df_q7['week'].unique()),
                         value='week', clearable=False,),
        ]), color="warning", outline=True), class_name='col-12'),
    ],
    className="mb-4",
)


cards = html.Div([row_1, row_2, row_3, row_4])
app.layout = html.Div(children=[
    html.H1(children='LastFM Dashboard',
            style={
                'textAlign': 'center',
            }),

    dbc.Container(children=[row_0, cards])



]
)

# fonction qui permet de prendre en compte la modification de 'year' et 'week' sur le front, tout en mettant a jour le graph


@app.callback(
    Output("graph", "figure"),
    Input("year", "value"),
    Input("week", "value"))
def generate_chart(year, week):
    df_chart = df_q5
    df_generate = df_chart.loc[(df_chart['week'] == week) & (
        df_chart['year'] == year)]
    fig3 = px.pie(df_generate, values='count', names='artist', hole=.3)

    return fig3

# fonction qui permet de prendre en compte la modification de 'year2' et 'week2' sur le front, tout en mettant a jour le graph


@app.callback(
    Output("graph2", "figure"),
    Input("year2", "value"),
    Input("week2", "value"))
def generate_chart2(year, week):
    df_chart = df_q6
    df_generate = df_chart.loc[(df_chart['week'] == week) & (
        df_chart['year'] == year)]

    fig3 = px.pie(df_generate, values='count', names='song', hole=.3)

    return fig3

# fonction qui permet de prendre en compte la modification de 'year3' et 'week3' sur le front, tout en mettant a jour le graph


@app.callback(
    Output("graph3", "figure"),
    Input("year3", "value"),
    Input("week3", "value"))
def generate_chart3(year, week):
    df_chart = df_q7
    df_generate = df_chart.loc[(df_chart['week'] == week) & (
        df_chart['year'] == year)]

    fig3 = px.pie(df_generate, values='count', names='album', hole=.3)

    return fig3


if __name__ == '__main__':
    app.run_server(debug=True)
