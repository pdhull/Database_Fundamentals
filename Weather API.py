# %%
import pandas as pd
import sqlalchemy as sa
import requests
#import json_normalize
import time
from datetime import datetime
#import schedule

# %%
cities = pd.read_csv('canadacities.csv')

# %%
relevant_columns_ca = ['city','lat','lng']
cities = cities[relevant_columns_ca]

# %%
cities

# %%
# Limiting this to 10 cities as we have limits on APIs
cities = cities.head(10)

# %% [markdown]
# ### Historical Data

# %%
db_secret={
    'drivername': 'postgresql+psycopg2',
    'host':'mmai5100postgres.canadacentral.cloudapp.azure.com',
    'port':'5432',
    'user':'rupaliw',
    'password':'2023!Schulich',
    'database':'rupaliw_db'
}
db_connection_url = sa.engine.URL.create(
    drivername = db_secret['drivername'],
    username = db_secret['user'],
    password = db_secret['password'],
    host = db_secret['host'],
    port = db_secret['port'],
    database = db_secret['database']
)
engine = sa.create_engine(db_connection_url)
with engine.connect() as connection:
    connection.execute('CREATE SCHEMA IF NOT EXISTS uploads')

# %%
location = 'Toronto'
timesteps = '1d'
units = 'metric'
apikey = 'bBfX0mo9rPaPr2sCstDcs82BU5HCTq7T'

# %%
api_url_history = 'https://api.tomorrow.io/v4/weather/history/recent?location={}&Timesteps={}&units={}&apikey={}'.format(location,timesteps,units,apikey)

# %%
api_response_history = requests.get(api_url_history)

# %%
history = api_response_history.json()

# %%
history

# %%
hourly_data = history["timelines"]["hourly"]

rows = []

for record in hourly_data:
    time = record["time"]
    values = record["values"]
    row = {"time": time}
    row.update(values)
    rows.append(row)

history_df = pd.DataFrame(rows)

# %%
history_df

# %%
history_df.info()

# %%
# Data ingestion Pipeline
def fetch_data():
    timesteps = '1d'
    units = 'metric'
    apikey = 'WITDyLD6qQQRVRT4OwHZCPCbbYK5dAu7'

    all_rows = []

    for location in cities['city']:
        api_url_history = 'https://api.tomorrow.io/v4/weather/history/recent?location={}&Timesteps={}&units={}&apikey={}'.format(location, timesteps, units, apikey)
        api_response_history = requests.get(api_url_history)
        history = api_response_history.json()
        print(history)
        hourly_data = history["timelines"]["hourly"]

        for record in hourly_data:
            time = record["time"]
            values = record["values"]
            row = {"time": time}
            row.update(values)
            all_rows.append(row)

    history_df = pd.DataFrame(all_rows)
    return history_df

def clean_data(data):
    data.fillna(0,inplace=True)
    relevant_columns = ['time','temperature','windSpeed','humidity']
    data = data[relevant_columns]
    return data

def feature_engineering(data):
    data['time'] = pd.to_datetime(data['time'])
    return data

def push_db(data):
    data.to_sql(
        name='historical_weather',#table name
        schema='uploads',
        con = engine,
        if_exists ='replace',
            index=False,
            method='multi',
            dtype={
                'time':sa.types.DATE,
                'temperature':sa.types.INTEGER,
                'windSpeed':sa.types.INTEGER,
                'humidity':sa.types.INTEGER
            }
        )


def pipeline():
    data = fetch_data()
    data = clean_data(data)
    data = feature_engineering(data)
    push_db(data)
    print("Pipeline executed successfully")
    
pipeline()

## Schedule the pipeline to run every day at a specific time - Commented out for now as these packages do not exist on this server
#schedule.every().day.at("09:00").do(pipeline)

## Keep running the scheduling loop
#while True:
    # schedule.run_pending()
    # time.sleep(1)


# %% [markdown]
# Since the limit has been reached for historical data, we are unable to run the pipeline but please review the code. 

# %% [markdown]
# ###  Current Weather

# %%
appid = '49151efa41b8c02c4a9062ccdcf2157c'
lat = 43.6548
lon = -79.3883

api_url = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}'.format(lat, lon, appid)

print(api_url)


# %%
api_response = requests.get(api_url)

# %%
api_response

# %%
data = api_response.json()
data

# %%
weather = data['weather']
main = data['main']

# %%
data

# %%
df = pd.json_normalize(data, 'weather', ['coord', 'base', 'main', 'visibility', 'wind', 'clouds', 'dt', 'sys', 'timezone', 'id', 'name', 'cod'],
                   record_prefix='weather_')

df_main = pd.json_normalize(data['main'])
df_wind = pd.json_normalize(data['wind'])
df_clouds = pd.json_normalize(data['clouds'])
df_coord = pd.json_normalize(data['coord'])
df_sys = pd.json_normalize(data['sys'])

df = pd.concat([df, df_main, df_wind, df_clouds, df_coord, df_sys], axis=1)


# %%
df

# %%
df.info()

# %%
# Data ingestion Pipeline
def fetch_data():
    all_rows=[]
    appid = '49151efa41b8c02c4a9062ccdcf2157c'

    result_df = pd.DataFrame()

    for index, row in cities.iterrows():
        lat = row['lat']
        lon = row['lng']
        api_url = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}'.format(lat, lon, appid)
        api_response = requests.get(api_url)
        data = api_response.json()

        df = pd.json_normalize(data, 'weather', ['coord', 'base', 'main', 'visibility', 'wind', 'clouds', 'dt', 'sys', 'timezone', 'id', 'name', 'cod'],
                       record_prefix='weather_')
        df_main = pd.json_normalize(data['main'])
        df_wind = pd.json_normalize(data['wind'])
        df_clouds = pd.json_normalize(data['clouds'])
        df_coord = pd.json_normalize(data['coord'])
        df_sys = pd.json_normalize(data['sys'])
        df_concat = pd.concat([df, df_main, df_wind, df_clouds, df_coord, df_sys], axis=1)

        df_main.columns = ['main_' + col for col in df_main.columns]
        df_wind.columns = ['wind_' + col for col in df_wind.columns]
        df_clouds.columns = ['clouds_' + col for col in df_clouds.columns]
        df_coord.columns = ['coord_' + col for col in df_coord.columns]
        df_sys.columns = ['sys_' + col for col in df_sys.columns]

        # Concatenating the DataFrames
        df_concat = pd.concat([df, df_main, df_wind, df_clouds, df_coord, df_sys], axis=1)
        print("df_concat columns:", df_concat.columns)
        
        all_rows.append(df_concat)

    result_df = pd.concat(all_rows, ignore_index=True)
    return result_df

def clean_data(data):
    data.fillna(0,inplace=True)
    relevant_columns = ['coord_lat','coord_lon','weather_main','main_humidity','main_temp_min','main_temp_max']
    data = data[relevant_columns]
    return data

def feature_engineering(data):
    data['avg_temp'] = (data['main_temp_min']+data['main_temp_max'])/2
    return data

def push_db(data):
    data.to_sql(
        name='current_weather',
        schema='uploads',
        con = engine,
        if_exists ='replace',
            index=False,
            method='multi',
            dtype={
                'lat':sa.types.DECIMAL(10,0),
                'lon':sa.types.DECIMAL(10,0),
                'weather_main':sa.types.VARCHAR(255),
                'humidity':sa.types.INTEGER,
                'temp_min':sa.types.DECIMAL(10,0),
                'temp_max':sa.types.DECIMAL(10,0),
                'avg_temp':sa.types.DECIMAL(10,0)
            }
        )


def pipeline():
    data = fetch_data()
    data = clean_data(data)
    data = feature_engineering(data)
    push_db(data)
    print("Pipeline executed successfully")
    
pipeline()

# Schedule the pipeline to run every day at a specific time
#schedule.every().day.at("09:00").do(pipeline)

# Keep running the scheduling loop
#while True:
    # schedule.run_pending()
    # time.sleep(1)


# %% [markdown]
# ### Forecast for 5 days:

# %%
# Data ingestion Pipeline
def fetch_data():
    appid = '49151efa41b8c02c4a9062ccdcf2157c'
    all_rows = []
    for index, row in cities.iterrows():
        lat = row['lat']
        lon = row['lng']
        api_url_2 = 'https://api.openweathermap.org/data/2.5/forecast?lat={}&lon={}&appid={}'.format(lat, lon, appid)
        api_response_2 = requests.get(api_url_2)
        data = api_response_2.json()

        list_data = data['list']
        rows = []

        for item in list_data:
            row = {
            'dt': item['dt'],
            'temp': item['main']['temp'],
            'feels_like': item['main']['feels_like'],
            'temp_min': item['main']['temp_min'],
            'temp_max': item['main']['temp_max'],
            'pressure': item['main']['pressure'],
            'sea_level': item['main']['sea_level'],
            'grnd_level': item['main']['grnd_level'],
            'humidity': item['main']['humidity'],
            'temp_kf': item['main']['temp_kf'],
            'weather_id': item['weather'][0]['id'],
            'weather_main': item['weather'][0]['main'],
            'weather_description': item['weather'][0]['description'],
            'weather_icon': item['weather'][0]['icon'],
            'clouds_all': item['clouds']['all'],
            'wind_speed': item['wind']['speed'],
            'wind_deg': item['wind']['deg'],
            'wind_gust': item['wind'].get('gust', None), 
            'visibility': item['visibility'],
            'pop': item['pop'],
            'sys_pod': item['sys']['pod'],
            'dt_txt': item['dt_txt']
            }
            rows.append(row)

        all_rows.extend(rows)

    df2 = pd.DataFrame(all_rows)
    return df2
    
def clean_data(data):
    data.fillna(0,inplace=True)
    relevant_columns = ['dt','temp_min','temp_max','temp','humidity','weather_description']
    data = data[relevant_columns]
    return data

def feature_engineering(data):
    data['dt']=pd.to_datetime(data['dt'])
    data['avg_temp'] = (data['temp_min']+data['temp_max'])/2
    return data

def push_db(data):
    data.to_sql(
        name='forecast_weather',
        schema='uploads',
        con = engine,
        if_exists ='replace',
            index=False,
            method='multi',
            dtype={
                'dt':sa.types.DATE,
                'temp_min':sa.types.DECIMAL(10,0),
                'temp_max':sa.types.DECIMAL(10,0),
                'temp':sa.types.DECIMAL(10,0),
                'humidity':sa.types.INTEGER,
                'weather_description':sa.types.VARCHAR(255),
            }
        )
    return data


def pipeline():
    data = fetch_data()
    data = clean_data(data)
    data = feature_engineering(data)
    db = push_db(data)
    print(db)
    print("Pipeline executed successfully")
    
pipeline()

# Schedule the pipeline to run every day at a specific time
#schedule.every().day.at("09:00").do(pipeline)

# Keep running the scheduling loop
#while True:
    # schedule.run_pending()
    # time.sleep(1)


# %% [markdown]
# ### Weather Alerts

# %%
cities

# %%
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

def process_url(url, dataframe):
    print(f'Processing URL: {url}')

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Skipping {url} (status code {response.status_code})")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # Iterate through the links on the page
    for a_tag in soup.find_all('a'):
        link = a_tag['href']
        if link.startswith('/'):
            continue

        file_url = url.rstrip('/') + '/' + link

        # If the link ends with '/', it's a folder, so call this function recursively
        if link.endswith('/'):
            process_url(file_url, dataframe)
        elif link.endswith('.cap'):
            print(f'Reading file: {file_url}')

            file_response = requests.get(file_url)
            if file_response.status_code != 200:
                print(f"Skipping {file_url} (status code {file_response.status_code})")
                continue

            root = ET.fromstring(file_response.text)
            
            # Define the namespace to use
            namespaces = {'cap': 'urn:oasis:names:tc:emergency:cap:1.2'}

            # Extract the polygon and the headline data
            polygons = [polygon.text for polygon in root.findall('.//cap:area/cap:polygon', namespaces=namespaces)]
            alerts = [alert.text for alert in root.findall('.//cap:info/cap:headline', namespaces=namespaces)]

            for polygon, headline in zip(polygons, alerts):
                dataframe.loc[len(dataframe)] = [file_url, polygon, headline]

columns = ['File URL', 'Polygon', 'Headline']
new_xml = pd.DataFrame(columns=columns)

url = 'https://dd.weather.gc.ca/alerts/cap'

process_url(url, new_xml)

# Print and save the DataFrame
new_xml.to_csv('weatheralerts.csv')


# %% [markdown]
# ### Since the above code took 30 minutes to run, we will skip running it again and just import the file we saved the DF to the first time. 

# %%
new_xml = pd.read_csv('weatheralerts.csv')

# %%
new_xml

# %%
polygons_list = []
for row in new_xml['Polygon']:
    coordinates = row.split()
    polygons_list.append([tuple(map(float, coordinate.split(','))) for coordinate in coordinates])

# Print or use the polygons_list
for polygon in polygons_list:
    print(polygon)


# %%
from shapely.geometry import Point, Polygon
def row_to_polygon(row):
    coordinates = row.split()
    points = [tuple(map(float, coordinate.split(','))) for coordinate in coordinates]
    return Polygon(points)

new_xml['polygon_object'] = new_xml['Polygon'].apply(row_to_polygon)

for polygon in new_xml['polygon_object']:
    print(polygon)

# %%
from shapely.geometry import Polygon, Point

def string_to_polygon(polygon_str):
    coordinates = [tuple(map(float, coord.split(','))) for coord in polygon_str.split()]
    return Polygon(coordinates)

# Apply the function to the 'polygon' column
new_xml['polygon_object'] = new_xml['Polygon'].apply(string_to_polygon)

columns = ['City', 'Coordinates', 'Headline']
new_df = pd.DataFrame(columns=columns)

cities['point_object'] = cities.apply(lambda row: Point(row['lat'], row['lng']), axis=1)

for idx_coord, row_coord in cities.iterrows():
    point_to_check = row_coord['point_object']
    city_name = row_coord['city']
    coordinates = (row_coord['lat'], row_coord['lng'])

    for idx_poly, row_poly in new_xml.iterrows():
        polygon = row_poly['polygon_object']
        headline = row_poly['Headline']

        if polygon.contains(point_to_check):
            new_df.loc[len(new_df)] = [city_name, coordinates, headline]
            break

# Now new_df contains the required information
print(new_df)


# %%
new_df = pd.read_csv('final.csv')

# %%
new_df.info()

# %%
new_df

# %%
# Data ingestion Pipeline
def fetch_data():
    # This function would include the code block which reads all files from the directory, parses them and checks if the point is in the polygon
    df2 = new_df
    return df2
    
def clean_data(data):
    # This would contain polygon steps but since we cannot import shapely, Polygon and other such packages on this server, skipping adding this to pipeline
    data.dropna(inplace=True)
    return data

def feature_engineering(data):
    # This would include splitting, creating point object and polygon object as shown in codeblocks above
    return data

def push_db(data):
    data.to_sql(
        name='weather_alerts',#table name
        schema='uploads',
        con = engine,
        if_exists ='replace',
            index=False,
            method='multi',
            dtype={
                'City':sa.types.VARCHAR(255),
                'Coordinates':sa.types.VARCHAR(255),
                'Headline':sa.types.VARCHAR(255),
            }
        )
    return data


def pipeline():
    data = fetch_data()
    data = clean_data(data)
    data = feature_engineering(data)
    db = push_db(data)
    print(db)
    print("Pipeline executed successfully")
    
pipeline()

# Schedule the pipeline to run every day at a specific time
#schedule.every().day.at("09:00").do(pipeline)

# Keep running the scheduling loop
#while True:
    # schedule.run_pending()
    # time.sleep(1)



