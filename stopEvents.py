import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

def getStopEvents():

    print("loading stop event data . . .")
    # Send a GET request to the website
    url = "http://www.psudataeng.com:8000/getStopEvents/"
    response = requests.get(url)

    # Create BeautifulSoup object
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all <h2> tags
    h2_tags = soup.find_all("h2")

    # Find the closest table for each <h2> tag
    table_data = []
    for h2_tag in h2_tags:
        event_string = h2_tag.text.strip()
        event_number = re.search(r'\d+', event_string)
        table = h2_tag.find_next("table")
        
        # Find all table rows within the current table
        rows = table.find_all("tr")
        
        # Extract the text from each row and add it to the table_data list
        for row in rows:
            cells = row.find_all("td")
            row_data = [event_number.group()] + [cell.text.strip() for cell in cells]
            table_data.append(row_data)


    df = pd.DataFrame(table_data, columns=['trip_id', 'vehicle_number', 'leave_time', 'train', 'route_number', 
                                           'direction', 'service_key', 'trip_number', 'stop_time', 'arrive_time',
                                           'dwell', 'location_id', 'door', 'lift', 'ons', 'offs','estimated_load',
                                           'maximum_speed', 'train_mileage','pattern_distance','location_distance',
                                           'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status'])
    df = df.replace(to_replace='None', value=np.nan).dropna()

    # print(df.to_json(orient= 'records'))
    return df


def getTripDf(df):

    df1 = pd.DataFrame.from_dict(df, orient='columns')
    df1 = df1.rename(columns={'route_number': 'route_id', 'vehicle_number': 'vehicle_id'})
    # print(df1.head)
    df2 = df1[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']]

    print(df2.head)
    # df2 = df2.rename(columns={'route_number': 'route_id', 'vehicle_number': 'vehicle_id'}, inplace = True)
    return df2

