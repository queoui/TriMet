import unittest
import datetime
import pandas as pd

class ValidateBreadCrumbData(unittest.TestCase):
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
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_existencevalidation'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_intrarecord'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_limit'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_meters'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_acttime'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_latitude'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_longitude'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_summary'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_notnegative'))
    suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_date'))
    return suite
