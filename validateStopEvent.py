import unittest
import datetime
import pandas as pd

class ValidateStopEventData(unittest.TestCase):
    def __init__(self, data, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        # self.df = pd.DataFrame(data, columns=['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP'])


    # # every EVENT_NO_TRIP, EVENT_NO_STOP, OPD_DATE and VEHICLE_ID is not null
    # def test_existencevalidation(self):
    #     for (colname, colval) in self.df.iterrows():
    #         self.assertIsNotNone(colval['EVENT_NO_TRIP']);
    #         self.assertIsNotNone(colval['EVENT_NO_STOP']);
    #         self.assertIsNotNone(colval['OPD_DATE']);
    #         self.assertIsNotNone(colval['VEHICLE_ID']);
    #         self.assertIsNotNone(colval['ACT_TIME']);

def suite(fetchedData):
    suite = unittest.TestSuite()
    # suite.addTest(ValidateBreadCrumbData(fetchedData, 'test_existencevalidation'))
    return suite
