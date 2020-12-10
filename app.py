import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from folium.plugins import MarkerCluster
import json
import os
import folium
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
community_geojson = 'Community Areas.geojson'
area_names_df = None

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
application = app.server

CRIMINAL_HEATMAP_PATH = './maps/criminal_heatmaps/'
SPECIAL_CRIME_HEATMAP_PATH = './maps/special_crime_heatmaps/'
SPECIAL_CRIME_MARKERS_PATH = './maps/special_crime_markers/'

CRIME_TYPES = ['OFFENSE INVOLVING CHILDREN', 'CRIMINAL SEXUAL ASSAULT', 'STALKING', 'PUBLIC PEACE VIOLATION', 'OBSCENITY', 
    'ARSON', 'GAMBLING', 'CRIMINAL TRESPASS', 'ASSAULT', 'LIQUOR LAW VIOLATION', 'MOTOR VEHICLE THEFT', 'THEFT', 
    'BATTERY', 'ROBBERY', 'HOMICIDE', 'PUBLIC INDECENCY', 'CRIM SEXUAL ASSAULT', 'HUMAN TRAFFICKING', 'INTIMIDATION', 
    'PROSTITUTION', 'DECEPTIVE PRACTICE', 'CONCEALED CARRY LICENSE VIOLATION', 'SEX OFFENSE', 'CRIMINAL DAMAGE', 
    'NARCOTICS', 'NON-CRIMINAL', 'OTHER OFFENSE', 'KIDNAPPING', 'BURGLARY', 'WEAPONS VIOLATION', 'OTHER NARCOTIC VIOLATION', 
    'INTERFERENCE WITH PUBLIC OFFICER']


app.layout = html.Div([

    html.H1("Chicago Criminal Map", style={'text-align': 'center'}),
    dcc.Slider(
        id='year-slider',
        min=2001,
        max=2020,
        value=2020,
        marks={str(year): str(year) for year in range(2001,2021)},
        step=None
    ),
    html.Br(),
    dcc.Loading(
        html.Iframe(id='crime-heatmap', width='100%', height='600')
    ),
    html.Br(),
    dcc.Dropdown(
        options=[ {'label': crime_type.lower(), 'value': crime_type} for crime_type in CRIME_TYPES ],
        value='GAMBLING',
        id='crime-selector'
    ), 
    html.Br(),
    html.Div(
        [
            html.Div([
                dcc.Loading(
                    html.Iframe(id='special-crime-heatmap', width='100%', height='500')
                )
            ], className="six columns"),
            
            html.Div([
                dcc.Loading(
                    html.Iframe(id='special-crime-markers',  width='100%',  height='500')
                )
            ], className="six columns"),
        ], 
    )
])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

def log(s, m):
    print('>'*m, s)

def generate_map(generator, map_path, params):

    if not os.path.exists(map_path):
        log(f'Cached map  {map_path} not found', 10)
        generator(map_path, *params)

    with open(map_path) as f:
        map = f.read()

    return map

@app.callback(
    Output(component_id='crime-heatmap', component_property='srcDoc'),
    Input(component_id='year-slider', component_property='value'))
def update_crime_heatmap(selected_year):
    map_path = CRIMINAL_HEATMAP_PATH + f'criminal_heatmap_{selected_year}.html'
    return generate_map(generate_criminal_heatmap, map_path, [selected_year])


@app.callback(
    Output(component_id='special-crime-heatmap', component_property='srcDoc'),
    [Input(component_id='year-slider', component_property='value'),
    Input(component_id='crime-selector', component_property='value')])
def update_special_crime_heatmap(selected_year, selected_crime):
    map_path = SPECIAL_CRIME_HEATMAP_PATH + f'_{selected_crime}_crime_heatmap_{selected_year}.html'
    return generate_map(generate_special_criminal_heatmap, map_path, [selected_year, selected_crime])


@app.callback(
    Output(component_id='special-crime-markers', component_property='srcDoc'),
    [Input(component_id='year-slider', component_property='value'),
    Input(component_id='crime-selector', component_property='value')])
def update_special_crime_markers(selected_year, selected_crime):
    map_path = SPECIAL_CRIME_MARKERS_PATH + f'_{selected_crime}_crime_markers_{selected_year}.html'
    return generate_map(generate_special_criminal_markers, map_path, [selected_year, selected_crime])


def generate_criminal_heatmap(map_path, year):

    log(f'Creating new heatmap {map_path}', 15)  
    crimes_by_area_df = data.filter(data.Year == year)\
        .groupBy('Community Area')\
        .count()\
        .toPandas()

    log(f'Creating new heatmap {map_path}: Data collected', 15)  

    global area_names_df
    if area_names_df is None:
        area_names_df = _get_areas_names(community_geojson)
    crimes_by_area = pd.merge(area_names_df, crimes_by_area_df)    

    m = folium.Map(location=[41.751657125,-87.650130681], zoom_start = 10)
    choropleth = folium.Choropleth(
        geo_data=community_geojson,
        data=crimes_by_area,
        columns=['Area Name', 'count'],
        key_on='feature.properties.community',
        legend_name='Crimes number',
        fill_color='OrRd',
        smooth_factor=0).add_to(m)

    choropleth.geojson.add_child(
        folium.features.GeoJsonTooltip(['community'],labels=False)
    )
    log(f'Creating new heatmap {map_path}: Map is ready', 15)  
    m.save(map_path)


def generate_special_criminal_heatmap(map_path, year, crime_type):

    log(f'Creating new special criminal heatmap {map_path}', 15)  
    per_year_dataset = data.filter(data.Year == year)
    special_crimes_by_area_df = per_year_dataset.filter(per_year_dataset['Primary Type'] == crime_type)\
        .groupBy('Community Area')\
        .count()\
        .toPandas()

    log(f'Creating new special criminal heatmap {map_path}: Data collected', 15)  
    global area_names_df
    if area_names_df is None:
        area_names_df = _get_areas_names(community_geojson)
    special_crimes_by_area = pd.merge(area_names_df, special_crimes_by_area_df) 

    m = folium.Map(location=[41.751657125,-87.650130681], zoom_start = 10)

    choropleth = folium.Choropleth(
        geo_data=community_geojson,
        data=special_crimes_by_area,
        columns=['Area Name', 'count'],
        key_on='feature.properties.community',
        legend_name=f'{crime_type} crimes number',
        fill_color='OrRd',
        smooth_factor=0).add_to(m)

    choropleth.geojson.add_child(
        folium.features.GeoJsonTooltip(['community'],labels=False)
    )

    log(f'Creating new special criminal heatmap {map_path}: Map is ready', 15)  
    m.save(map_path)


def generate_special_criminal_markers(map_path, year, crime_type):

    log(f'Creating new special criminal markers {map_path}', 15)  
    per_year_dataset = data.filter(data.Year == year)
    special_type_crimes =  per_year_dataset.filter(per_year_dataset['Primary Type'] == crime_type)\
        .collect()

    log(f'Creating new special criminal markers {map_path}: Data collected', 15)  

    m = folium.Map(location=[41.751657125,-87.650130681], zoom_start = 10)
    marker_cluster = MarkerCluster().add_to(m)
    for crime in special_type_crimes:
        folium.CircleMarker(
            location=[crime['Latitude'], crime['Longitude']],
            popup=crime['Date'],
            radius=10,
            tooltip=crime['Primary Type'],
            color='gray', 
            fill_color='orange',
            fill_opacity = 0.95
        ).add_to(marker_cluster)

    log(f'Creating new special criminal markers {map_path}: Map is ready', 15)  
    m.save(map_path)


def _get_areas_names(community_geojson):
      
    with open(community_geojson) as f:
        community_dict = json.load(f)
    
    community_dict = community_dict['features']
    denominations_dict = dict()
    for i in range(len(community_dict)):
        area_number = int(community_dict[i]['properties']['area_numbe'])
        denominations_dict[area_number] = (community_dict[i]['properties']['community'])
        
    area_names_df = pd.DataFrame.from_dict(
        {
            'Community Area': denominations_dict.keys(),
            'Area Name': denominations_dict.values()
        } 
    )
    return area_names_df


if __name__ == '__main__':

    log('init spark', 5)
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("ChicagoCriminalProject")\
        .getOrCreate()  

    log('init dataset', 5)
    data = spark.read.csv("Crimes_-_2001_to_Present.csv", inferSchema=True, header =True)

    log('init data', 5)
    data = data\
        .dropna(subset=('latitude','longitude','Date'))\
        .withColumn("Date", to_timestamp(data.Date, 'MM/dd/yyyy hh:mm:ss a'))\
        .select('Date', 'Year', 'Primary Type', 'Description', 'Location Description',
         'Arrest', 'Domestic', 'Community Area', 'Latitude', 'Longitude')

    # export PYTHONPATH=/usr/local/spark/python/lib/py4j-0.10.9-src.zip:/usr/local/spark/python:
    app.run_server(debug=False)
