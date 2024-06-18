from kafka import KafkaProducer
import os, sys, json

# export PYTHONPATH=.
sys.path.insert(0, os.getcwd())

from src.reader.reader import read_sample_files

if __name__ == '__main__':

    files = read_sample_files('/home/ols/telecom-messages-correlation/messages-sample/SS7/ISUP')

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    for f in files:
        with open(f,mode="r") as file_content:
            producer.send(topic='simulator', value=file_content.read().encode('UTF-8'))
            print(f)
