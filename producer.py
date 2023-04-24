#!/usr/bin/env python
import datetime
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import json
import urllib3

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    todaysFile = datetime.date.today().strftime("%m-%d-%Y-") + "TriMet.json"
    topic = "trimet-topic"
    f = open(todaysFile, "r")
    data = json.loads(f.read())

    count = 1
    for d in data:
        key = 'key: ' + str(count)
        producer.produce(topic, json.dumps(d).encode('utf-8'), key, callback = print(count))
        count += 1
        if(count %10000 == 0):
            producer.flush()

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
