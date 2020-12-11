# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 19:38:07 2020

@author: mahdi
"""

# -*- coding: utf-8 -*-
#%%
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from flask import Flask
#%%
engine = create_engine('sqlite:///Resources/hawaii.sqlite')
#%%
app = Flask(__name__)

#%%
@app.route('/')
def Welcome():
    Welcome = 'Welcome! all routes are available as below'
    return(Welcome)
#%%
@app.route('/api/v1.0/precipitation')
def prcp():
    conn = engine.connect()
    query = f'''
        SELECT 
            date,
            AVG(prcp) as avg_prcp
        FROM
            measurement
        WHERE
            date >= (SELECT DATE(MAX(date),'-1 year') FROM measurement)
        GROUP BY
            date
        ORDER BY 
            date
    '''
    # Save the query results as a Pandas DataFrame and set the index to the date column
    prcp_df = pd.read_sql(query, conn)
    # Convert the date column to date
    prcp_df['date'] = pd.to_datetime(prcp_df['date'])
    # Sort the dataframe by date
    prcp_df.sort_values('date')
    prcp_json = prcp_df.to_json(orient='records', date_format = 'iso')
    conn.close()
    return prcp_json
#%%
@app.route('/api/v1.0/stations')
def station():
    conn = engine.connect()
    query = "SELECT Station FROM measurement"
    station_df = pd.read_sql(query, conn)
    station_json = station_df.to_json(orient='records' )
    conn.close()
    return station_json



#%%
@app.route('/api/v1.0/tobs')
def temp():
    conn = engine.connect()
    query = "SELECT date, tobs FROM measurement WHERE station = 'USC00519281' "
    temp_date = pd.read_sql(query, conn)
    temp_date_json = temp_date.to_json(orient = 'records', date_formate = 'iso')
    conn.close()
    return temp_date_json

#%%
@app.route('/api/v1.0/<start_date>')
def start(start_date):
    conn = engine.connect()
    query  = f'''
        SELECT	
            MIN(tobs),
            MAX(tobs),
            AVG(tobs)
        FROM	
            measurement
        WHERE
            date >= '{start_date}'
    '''
    start_date_df = pd.read_sql(query, conn)
    start_date_json = start_date_df.to_json(orient='records', date_formate= 'iso')
    return start_date_json 
#%%
@app.route('/api/v1.0/<start>/<end>')
def end_start_date(start, end):
    conn = engine.connect()
    
    query = f'''
        SELECT	
            MIN(tobs) AS min_tobs,
            MAX(tobs) AS max_tobs,
            AVG(tobs) AS avg_tobs
        FROM	
            measurement
        WHERE
            date BETWEEN '{start}' AND '{end}'
    '''
    
    print(query)
    
    end_start_date_df= pd.read_sql(query, conn)
    end_start_json = end_start_date_df.to_json(orient='records', date_format='iso')
    return end_start_json 

#%%
if __name__ == '__main__':
    app.run(debug=True)    
#%%
'''
/api/v1.0/precipitation
Convert the query results to a dictionary using date as the key and prcp as the value.
Return the JSON representation of your dictionary.
/api/v1.0/stations
Return a JSON list of stations from the dataset.
'''