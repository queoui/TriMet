
import datetime
import pandas as pd

def timeStamp(row, *args):
    opd_date = row['OPD_DATE']
    act_time = row['ACT_TIME']
    
    date = datetime.datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S').date()
    time = datetime.timedelta(seconds=act_time) 
    timestamp = datetime.datetime.combine(date, datetime.datetime.min.time()) + time

    return timestamp


def getspeed(fetchedData):
    df = pd.DataFrame(fetchedData, columns=['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP'])
    df['TIMESTAMP'] = df.apply(timeStamp, axis=1, args=(df['OPD_DATE'], df['ACT_TIME']))
    df['dMETERS'] = df['METERS']
    df['dTIMESTAMP'] = df['TIMESTAMP'].diff().dt.total_seconds()

    get_speed = lambda x: x['dMETERS'] / x['dTIMESTAMP']

    df['SPEED'] = df.apply(get_speed, axis=1)
    df.loc[0, 'SPEED'] = df.loc[1, 'SPEED']
    df2 = df.drop(['dMETERS'], axis = 1)
    df2 = df.drop(['dTIMESTAMP'], axis = 1)

    return df2


def getTripDf(df):
    df1 = pd.DataFrame(columns=['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'])
    df1['trip_id'] = df['EVENT_NO_TRIP']
    df1['vehicle_id'] = df['VEHICLE_ID']

    return df1


def getBreadCrumbDf(df):
    df1 = pd.DataFrame(columns=['tstamp', 'latitude', 'longitude', 'speed', 'trip_id'])
    df1['tstamp'] = df['TIMESTAMP']
    df1['latitude'] = df['GPS_LATITUDE']
    df1['longitude'] = df['GPS_LONGITUDE']
    df1['speed'] = df['SPEED']
    df1['trip_id'] = df['EVENT_NO_TRIP']

    return df1