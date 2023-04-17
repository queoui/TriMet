import urllib3
import datetime
import json
from confluent_kafka import Producer
from uuid import uuid4

http = urllib3.PoolManager()
todaysDate = datetime.date.today().strftime("%m-%d-%Y-")
todaysFile = todaysDate+"TriMet.json"

print("Reaching out to TriMet data source .. ", end='', flush = True)

def requestTrimetData():
    return(http.request('GET' , 'http://psudataeng.com:8000/getBreadCrumbData'))

def TriMetToText(resp):
    if(resp.status == 200):
        print('Successful!')
        print("New file "+ todaysFile+" created.")
        with open(todaysFile, 'wb') as writeOut:
            writeOut.write(resp.data)
        writeOut.close()
    else:
        print('Unsuccessful ..')
        print('Something went wrong, no new file created.')

def parseData():
    jsonOut = open(todaysFile, "rb")
    jsonData = json.load(jsonOut)
    # jsonOut.close()
    return jsonData

def kafkaSend(jsonData):

    kafka_topic_name = "trimet-topic"   
    
    mysecret = "IBuBuvrDzt+O+EAVLXq2tlBw5qo5H/MP+fV0ZvcZjRZApYnwD2aH6SdrLBTcKSLV"
    #you can call remote API to get JKS password instead of hardcoding like above
    
    print("Starting Kafka Producer")   
    conf = {
            'bootstrap.servers' : 'pkc-lgk0v.us-west1.gcp.confluent.cloud:9092',
            'security.protocol' : 'SSL',
            'ssl.keystore.password' : mysecret,
            # 'ssl.keystore.location' : './certkey.p12'
            }
            
    print("connecting to Kafka topic...")
    producer1 = Producer(conf)
    producer1.poll(0)
    
    try:
        for snippets in jsonData:
            producer1.produce(kafka_topic_name, json.dumps(snippets).encode('utf-8'), on_delivery=delivery_callback)
            producer1.flush()
        
    except Exception as ex:
        print("Exception happened :",ex)
        
    print("\n Stopping Kafka Producer")
def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
 

resp = requestTrimetData()
TriMetToText(resp)
jsonData = parseData()
kafkaSend(jsonData)

