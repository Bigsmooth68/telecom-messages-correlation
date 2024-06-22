# export PYTHONPATH=.
import sys, os
sys.path.insert(0, os.getcwd())

from kafka import KafkaProducer
import json, time, argparse
from src.reader.reader import read_messages_files

def current_milli_time():
    return round(time.time() * 1000)

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
        # Get message timestamp and update it with current time
        m_time = m['timestamp']
        m['timestamp'] = current_milli_time()

        # Determine how much time we should wait between messages
        sleep_time = m_time - previous_time
        print(f'{current_milli_time()} - Sleeping {sleep_time}s')
        time.sleep(sleep_time)
        previous_time = m_time # Move time forward based on current time

        # Produce message
        producer.send(topic='simulator', value=json.dumps(m).encode('UTF-8'))
        print(f'{current_milli_time()} - Message sent {m["MessageType"]}')
