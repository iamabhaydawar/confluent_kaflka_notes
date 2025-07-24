import datetime
import time
from decimal import *
from uuid import uuid4,UUID
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer 
import pandas as pd


# Kafka configuration dictionary
kafka_config = {
    'bootstrap.servers': 'pkc-57jzz.southcentralus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'V5DSCECFW2C7TAZF',  # Replace with your API key
    'sasl.password': 'BrRa/GlSjcN8fIel9P6DuPacSTR75W60ghHx8gCkuPfRl8Unx38e7k4D1EX49JGn'  # Replace with your API secret
}

# Schema Registry client setup configuration
schema_registry_client = SchemaRegistryClient(
    {
        'url': 'pkc-57jzz.southcentralus.azure.confluent.cloud:9092',
        'basic.auth.user.info': '{}:{}'.format("V5DSCECFW2C7TAZF","BrRa/GlSjcN8fIel9P6DuPacSTR75W60ghHx8gCkuPfRl8Unx38e7k4D1EX49JGn")  # Replace with your Schema Registry credentials
    }
)


#Fetch the latest avro schema for the value
subject_name='retail_data_dev-value'
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("--------------------------------")
print("Schema from Schema Registry:")
print(schema_str)


#Create Avro Serializer for the value
#key_serializer=AvroSerializer(schema_registry_client, schema_str="type:string")
# Create serializers
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

#define the SerializingProducer
producer = SerializingProducer(
    {
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.serializer': key_serializer, #key will be serialized as a string
        'value.serializer': avro_serializer, #value will be serialized  as avro
    }
)

# Define the delivery report callback
def delivery_report(err, msg):
    '''
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
         In the delivery report, callback the Message.key() and Message.value()
         will be the binary format as encoded by any configured Serializers and not the same object that was passed to produce().
         If you wish to pass the original objects for key and value to delivery report callback, we recommend a bound callback 
         or lambda where you pass the objects along.
    '''

    if err is not None:
        print('Message delivery failed for User record: {}:{}'.format(msg.key(),err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(),msg.topic(),msg.partition(),msg.offset()))

#load the data from the csv file
df=pd.read_csv('retail_data.csv')
df=df.fillna('null')
print(df.head(5))
print("--------------------------------")
#Iterate  over dataframe rows and produce to kafka topic
for index,row in df.iterrows():
    #Create a dictionary from the row values
    data_value=row.to_dict()
    print(data_value)
    #Produce the data to the kafka topic
    # producer.produce(topic='retail_data_dev', key=str(index), value=data_value, on_delivery=delivery_report)
    # producer.flush()
    time.sleep(2)
    #break
# print("All data successfully published to kafka topic")