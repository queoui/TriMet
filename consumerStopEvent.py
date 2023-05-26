#!/usr/bin/env python
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
import unittest
import datetime
import pandas as pd
from sqlalchemy import create_engine

import stopEvents


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
    topic = "trimet-stopEvent"
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
                    #validate data
                    # validateData = validateBreadCrumb.ValidateData(fetchedData)
                    # runner = unittest.TextTestRunner()
                    # runner.run(validateBreadCrumb.suite(fetchedData))
                    
                    #update trip info
                    tripDf = stopEvents.getTripDf(fetchedData)
                    
                    # create a database connection
                    engine = create_engine('postgresql://postgres:postgres@localhost/postgres')

                    # save the dataframe to a new table in the database

                    tripDf.to_sql("tripstopevent", engine, if_exists="append")
                    # fetchedData.to_sql("stopevent", engine, if_exists="append")
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

                #Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
