#!/usr/bin/env python
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
import unittest
import datetime
import pandas as pd
from sqlalchemy import create_engine

class ValidateData(unittest.TestCase):
    def __init__(self, data, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.df = pd.DataFrame(data, columns=['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP'])
        
    # test if number of records do not exceed 100 million
    def test_summary(self):
        self.assertTrue(self.df.size <= 100000000)

    # every EVENT_NO_TRIP, EVENT_NO_STOP, OPD_DATE and VEHICLE_ID is not null
    def test_existencevalidation(self):
        for (colname, colval) in self.df.iterrows():
            self.assertIsNotNone(colval['EVENT_NO_TRIP']);
            self.assertIsNotNone(colval['EVENT_NO_STOP']);
            self.assertIsNotNone(colval['OPD_DATE']);
            self.assertIsNotNone(colval['VEHICLE_ID']);
            self.assertIsNotNone(colval['ACT_TIME']);

    # every GPS_LATITUDE has a GPS_LONGITUDE assigned to it
    def test_intrarecord(self):  
        for (colname, colval) in self.df.iterrows(): 
            self.assertEqual(self.assertIsNotNone(colval['GPS_LATITUDE']), self.assertIsNotNone(colval['GPS_LONGITUDE']));

    # every crash occured in the year 2023
    def test_limit(self):
        for (colname, colval) in self.df.iterrows(): 
            year = datetime.datetime.strptime(colval['OPD_DATE'], '%d%b%Y:%H:%M:%S').year
            self.assertTrue(year == 2023);

    # meters value should not be exceed 1 million
    def test_meters(self):
        for (colname, colval) in self.df.iterrows(): 
            self.assertTrue(colval['METERS'] <= 1000000);

    # act time value should not be greater than 86400 and not negative
    def test_acttime(self):
        for (colname, colval) in self.df.iterrows(): 
            self.assertTrue(colval['ACT_TIME'] <= 86400);
            self.assertTrue(colval['ACT_TIME'] >= 0);

    # check if the latitude is between -90 and +90 which is Earth's value
    def test_latitude(self):  
        for (colname, colval) in self.df.iterrows():  
            self.assertEqual(colval['GPS_LATITUDE'] <= 90, colval['GPS_LATITUDE'] >= -90)

    # check if the longitude is between -180 and +180 which is Earth's value
    def test_longitude(self):  
        for (colname, colval) in self.df.iterrows():
            self.assertEqual(colval['GPS_LONGITUDE'] <= 180, colval['GPS_LONGITUDE'] >= -180)
    
    # check if the values are not negative
    # every EVENT_NO_TRIP, EVENT_NO_STOP, METERS, GPS_SATELLITES and VEHICLE_ID is not negative
    def test_notnegative(self):
        for (colname, colval) in self.df.iterrows():
            self.assertTrue(colval['EVENT_NO_TRIP'] >= 0);
            self.assertTrue(colval['EVENT_NO_STOP'] >= 0);
            self.assertTrue(colval['VEHICLE_ID'] >= 0);
            self.assertTrue(colval['METERS'] >= 0);
            self.assertTrue(colval['GPS_SATELLITES'] >= 0);

    # OPD_DATE cannot be a future date
    def test_date(self):
        for (colname, colval) in self.df.iterrows():
            opd_date = datetime.datetime.strptime(colval['OPD_DATE'], "%d%b%Y:%H:%M:%S")
            self.assertFalse(opd_date > datetime.datetime.now());


def suite(fetchedData):
    suite = unittest.TestSuite()
    suite.addTest(ValidateData(fetchedData, 'test_existencevalidation'))
    suite.addTest(ValidateData(fetchedData, 'test_intrarecord'))
    suite.addTest(ValidateData(fetchedData, 'test_limit'))
    suite.addTest(ValidateData(fetchedData, 'test_meters'))
    suite.addTest(ValidateData(fetchedData, 'test_acttime'))
    suite.addTest(ValidateData(fetchedData, 'test_latitude'))
    suite.addTest(ValidateData(fetchedData, 'test_longitude'))
    suite.addTest(ValidateData(fetchedData, 'test_summary'))
    suite.addTest(ValidateData(fetchedData, 'test_notnegative'))
    suite.addTest(ValidateData(fetchedData, 'test_date'))
    return suite


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
    df['dMETERS'] = df['METERS'].diff()
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


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "trimet-topic"
    consumer.subscribe([topic], on_assign=reset_offset)

    fetchedData = []

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                if len(fetchedData) > 0:
                    validateData = ValidateData(fetchedData)
                    runner = unittest.TextTestRunner()
                    runner.run(suite(fetchedData))

                    df = getspeed(fetchedData)

                    tripDf = getTripDf(df)
                    breadcrumbDf = getBreadCrumbDf(df)

                    # create a database connection
                    engine = create_engine('postgresql://postgres:postgres@localhost/postgres')

                    # save the dataframe to a new table in the database
                    tripDf.to_sql("Trip", engine, if_exists="append")
                    breadcrumbDf.to_sql("BreadCrumb", engine, if_exists="append")

                    print('Data added in DB')

                    fetchedData = []
                    cmdlist = []
                else:
                    print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Decode the message value
                fetchedData.append(json.loads(msg.value()))

                # Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
