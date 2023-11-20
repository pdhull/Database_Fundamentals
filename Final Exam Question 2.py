#!/usr/bin/env python
# coding: utf-8

# # FINAL EXAM - QUESTION 2 

# We start by importing the basic libraries.

# In[1]:


import pandas as pd
import sqlalchemy as sa
import requests


# resource_id is a unique identifier for a specific data resource. limit sets the maximum number of records to retrieve (1000), and offset determines the starting point (0) for data retrieval within the resource.

# In[2]:


resource_id = '8a89caa9-511c-4568-af89-7f2174b4378c'
limit = 1000
offset = 0


# The api_url variable stores a URL that constructs a request to access data from a specific resource using the given resource_id, limit, and offset values. The print(api_url) statement displays the complete API URL, encapsulating the resource, limit, and offset information, which is used to retrieve data from the specified source.

# In[3]:


api_url = 'https://data.ontario.ca/api/3/action/datastore_search?resource_id={}&limit={}&offset={}'.format(resource_id,limit,offset)

print(api_url)


# The api_response variable holds the response obtained by sending an HTTP GET request to the URL stored in api_url, which corresponds to a data resource. This response contains the data or information retrieved from the API endpoint.

# In[4]:


api_response=requests.get(api_url)
api_response


# The variable data contains the JSON-formatted content extracted from the api_response, making the retrieved data accessible for further processing in Python.

# In[5]:


data=api_response.json()
data


# Within the JSON content stored in the data variable, data['result']['records'] provides access to the array of records present in the 'result' section, allowing you to work with the actual data retrieved from the API response.

# In[6]:


data['result']['records']


# The Pandas DataFrame df is created using the data from the JSON array within data['result']['records'], enabling structured manipulation and analysis of the retrieved records.

# In[7]:


df=pd.DataFrame(data['result']['records'])
df


# The df.info() call provides a summary of the DataFrame df, displaying concise information about the data types, non-null counts, and memory usage of each column, aiding in understanding the structure and characteristics of the data.

# In[8]:


df.info()


# The df.head(5) command displays the first five rows of the DataFrame df, offering a quick preview of the retrieved data from the JSON records.

# In[9]:


df.head(5)


# The df.tail() command displays the last few rows of the DataFrame df, offering a glimpse of the final records in the retrieved JSON data.

# In[10]:


df.tail()


# The df.dtypes command provides the data types of each column in the DataFrame df, giving insight into how the data is stored and allowing you to understand the nature of the information in each column.

# In[11]:


df.dtypes


# The code converts the values in the 'report_date' column of the DataFrame df to datetime format using the pd.to_datetime() function, enabling proper handling and manipulation of date-related data.

# In[12]:


df['report_date'] = pd.to_datetime(df['report_date'])


# The code fills any missing (NaN) values in the 'total_individuals_at_least_one' column of the DataFrame df with zeros (0) and then converts the values in that column to integers using .astype(int), ensuring consistent data type and handling for further analysis or calculations.

# In[13]:


df['total_individuals_at_least_one'] = df['total_individuals_at_least_one'].fillna(0).astype(int)


# New columns are added to the DataFrame df: 'reported_new_doses_all' calculates the daily new doses administered, and 'reported_new_doses_unvaccinated' calculates new doses for unvaccinated individuals. The resulting DataFrame final_data includes selected columns for these metrics.

# In[14]:


df['reported_new_doses_all']= df['total_doses_administered'].diff(1)
df['reported_new_doses_unvaccinated'] = (df['total_doses_administered'] - df['total_individuals_at_least_one']).diff(1)
# Selecting relevant columns for the final table
final_data = df[['report_date', 'reported_new_doses_all', 'reported_new_doses_unvaccinated']]


# The DataFrame final_exam is assigned the values and structure of the DataFrame final_data, effectively creating a new DataFrame with the same content for potential further processing or analysis.

# In[15]:


final_exam=final_data


# The term "final_exam" refers to the DataFrame that has been created and assigned the values and structure of the DataFrame final_data. This DataFrame likely contains columns such as 'report_date', 'reported_new_doses_all', and 'reported_new_doses_unvaccinated', which were selected from the original data for specific analysis or reporting purposes.

# In[16]:


final_exam


# This code establishes a PostgreSQL database connection using the db_connection_url created from the db_secret details. A new schema named 'pipeline' is created within the database. The Pandas DataFrame final_exam is transferred into the database table 'covid_on_vaccine_data' within the 'pipeline' schema using the SQLAlchemy to_sql() method, replacing any existing data. This enables the storage of the final_exam data in the database for further use or analysis.

# In[17]:


db_secret = {
    'drivername': 'postgresql+psycopg2',
    'host' : 'mmai5100postgres.canadacentral.cloudapp.azure.com',
    'port' : '5432',
    'username' : 'pdhull',
    'password' : '2023!Schulich',
    'database' : 'pdhull_db'
}

db_connection_url = sa.engine.URL.create(
    drivername = db_secret['drivername'],
    username = db_secret['username'],
    password = db_secret['password'],
    host = db_secret['host'],
    port = db_secret['port'],
    database = db_secret['database']
)

db_connection_url

engine = sa.create_engine(db_connection_url)

with engine.connect() as connection:
    connection.execute('CREATE SCHEMA IF NOT EXISTS pipeline')

final_exam.to_sql(
    name = 'covid_on_vaccine_data', 
    schema = 'final_exam',
    con = engine,
    if_exists = 'replace',
    index = False,
    method = 'multi',
)


# # END
