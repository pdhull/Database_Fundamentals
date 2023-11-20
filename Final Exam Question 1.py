#!/usr/bin/env python
# coding: utf-8

# #                                       FINAL EXAM - QUESTION 1

# We will start by importing basic libraries in our Jupyter server.
# -Pandas is for data manipulation and analysis with tabular data.
# -SQLAlchemy is for working with relational databases and performing database operations.
# -Requests is for making HTTP requests to web servers and fetching data from URLs.

# In[ ]:


import pandas as pd
import sqlalchemy as sa
import requests


# The db_secret dictionary stores connection information for a PostgreSQL database: it specifies the driver, server host, port, username, password, and database name. These details are crucial for establishing a secure connection and interacting with the specified PostgreSQL database using Python libraries like SQLAlchemy.

# In[2]:


db_secret = {
    'drivername' : "postgresql+psycopg2",
    'host': "mmai5100postgres.canadacentral.cloudapp.azure.com",
    'port': "5432",
    'user': "pdhull",
    'password': '2023!Schulich',
    'database': 'mban_db'
}


# The db_connection_url is created using SQLAlchemy's URL.create() function, incorporating the connection details from the db_secret dictionary. This URL represents the precise address and authentication information needed to connect to a PostgreSQL database. It's a compact yet comprehensive representation that can be used by SQLAlchemy to establish a connection to the specified database server, facilitating database interactions and operations through Python code.

# In[3]:


db_connection_url = sa.engine.URL.create(
drivername  = db_secret['drivername'],
    username= db_secret['user'],
    password = db_secret['password'],
    host = db_secret['host'],
    port = db_secret['port'],
    database= db_secret['database']
)


# The engine is generated using SQLAlchemy's create_engine() with the prepared db_connection_url, establishing a direct link between Python and the PostgreSQL database for streamlined data manipulation and querying.

# In[5]:


engine = sa.create_engine(db_connection_url)


# Within the SQLAlchemy connection managed by engine.connect(), the code uses Pandas' read_sql() to fetch all data from the 'dimensions.date_dimension' table in the PostgreSQL database. The retrieved data is stored in a Pandas dataframe named data for further analysis in Python.

# In[6]:


with engine.connect() as connection: 
    data = pd.read_sql(sql ='SELECT * FROM dimensions.date_dimension;', con=connection  )


# This code displays the first few rows of the Pandas dataframe named data, providing a quick preview of the fetched data from the PostgreSQL database table.

# In[7]:


data.head()


# The term "data" refers to the Pandas dataframe that was previously created and populated with the retrieved information from the PostgreSQL database table 'dimensions.date_dimension'.

# In[8]:


data


# The code data.info() provides an overview of the Pandas dataframe named data, displaying concise information about the data types, non-null counts, and memory usage of each column. This summary is useful for understanding the structure and characteristics of the data in the dataframe.

# In[9]:


data.info()


# The db_secret dictionary holds essential connection details for accessing a PostgreSQL database: it specifies the driver, server host, port, authentication username and password, and the name of the target database. These credentials are vital for establishing a secure connection and conducting interactions with the specified PostgreSQL database, facilitating tasks such as querying and data manipulation through Python.

# In[10]:


db_secret = {
    'drivername' : "postgresql+psycopg2",
    'host': "mmai5100postgres.canadacentral.cloudapp.azure.com",
    'port': "5432",
    'username': "pdhull",
    'password': '2023!Schulich',
    'database': 'pdhull_db'
}


# The db_connection_url is generated using SQLAlchemy's URL.create() function, incorporating the connection details from the db_secret dictionary. This URL encapsulates the precise information required to establish a connection to a PostgreSQL database, encompassing the driver, authentication credentials, server location, and the target database's name. This compact URL serves as a streamlined way to configure a connection and allows SQLAlchemy to seamlessly interact with the designated database server using these parameters.

# In[11]:


db_connection_url=sa.engine.URL.create(
    drivername   = db_secret['drivername'],
    username     = db_secret['username'],
    password     =db_secret['password'],
    host         =db_secret['host'],
    port         =db_secret['port'],
    database     =db_secret['database']
)


# The code would show the complete database connection URL, combining driver, credentials, server details, and database name, essential for connecting to the PostgreSQL database.

# In[12]:


print(db_connection_url)


# This code uses SQLAlchemy to establish a connection engine to a PostgreSQL database using the provided db_connection_url, facilitating database interactions through Python.

# In[13]:


from sqlalchemy import create_engine, text
engine = create_engine(db_connection_url)


# Within a managed engine connection, the code employs execute() to create a new schema named 'final_exam' in the connected PostgreSQL database, ensuring it exists if not already present. This schema can serve as a designated space to organize and manage related database objects.

# In[15]:


with engine.connect() as connection:
    connection.execute('CREATE SCHEMA IF NOT EXISTS final_exam;')


# This code section transfers the data from the Pandas dataframe data to a PostgreSQL database table named 'date_dimension' within the 'final_exam' schema using SQLAlchemy. The data is inserted into the table, replacing any existing data if necessary (due to 'if_exists'='replace'). The specified column data types are enforced during the insertion, ensuring compatibility between the dataframe and the database. This process enables seamless migration of the data for analysis and querying within the database.

# In[16]:


# pull the data to sql
data.to_sql(
    name='date_dimension',
    schema='final_exam',
    con=engine,
    if_exists='replace',
    index=False,
    method='multi',
    dtype={
    'sk_date': sa.types.INTEGER,
    'date': sa.types.DATE,
    'day_name': sa.types.String,
    'day_of_month': sa.types.INTEGER,
    'day_of_year': sa.types.INTEGER,
    'month': sa.types.INTEGER,
    'month_name': sa.types.String,
    'year': sa.types.INTEGER,
    'year_week': sa.types.String,
    'week': sa.types.String,
    'running_week': sa.types.INTEGER,
    'year_quarter': sa.types.String,
    'quarter': sa.types.String,
    'running_quarter': sa.types.INTEGER
    }
)


# # END
