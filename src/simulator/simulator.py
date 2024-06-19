from kafka import KafkaProducer
import os, sys, json, time
import argparse

# export PYTHONPATH=.
sys.path.insert(0, os.getcwd())

def current_milli_time():
    return round(time.time() * 1000)

from src.reader.reader import read_messages_files

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("PATH",type=str,help="Path of files to read and push to Kafka")
    args = parser.parse_args()
    path = args.PATH

    # Get list of messages
    messages = read_messages_files(path)

    # Get Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    # Play messages
    previous_time = 0
    for m in messages:
        m_time = m['timestamp']

        sleep_time = m_time - previous_time
        print(f'{current_milli_time()} - Sleeping {sleep_time}s')
        time.sleep(sleep_time)
        previous_time = m_time

        m['timestamp'] = current_milli_time()
        producer.send(topic='simulator', value=json.dumps(m).encode('UTF-8'))
        print(f'{current_milli_time()} - Message sent {m["MessageType"]}')
