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
    parser.add_argument("--broker", type=str, help="URL to Kafka Broker. Default localhost:9092", default="localhost:9092")
    parser.add_argument("--repeat", "-r", action='store_true')
    parser.add_argument("PATH", type=str, help="Path of files to read and push to Kafka")
    args = parser.parse_args()
    path = args.PATH
    bs = args.broker
    repeat = args.repeat

    # Get list of messages
    messages = read_messages_files(path)

    # Get Kafka producer
    try:
        producer = KafkaProducer(bootstrap_servers=bs)
        print(f"Connected to {bs}")
    except:
        print(f'Not able to connect')
        exit(1)

    previous_time = 0    
    while True:
        # Play messages

        for m in messages:
            local_messsage = m.copy()
            # Get message timestamp and update it with current time
            m_time = local_messsage['timestamp']
            local_messsage['timestamp'] = current_milli_time()

            # Determine how much time we should wait between messages
            sleep_time = m_time - previous_time
            sleep_time = 0 if sleep_time < 0 else sleep_time # Prevent negativ value on loop

            print(f'{current_milli_time()} - Sleeping {sleep_time}s')
            time.sleep(sleep_time)
            previous_time = m_time # Move time forward based on current time

            # Produce message
            producer.send(topic='simulator', value=json.dumps(local_messsage).encode('UTF-8'))
            print(f'{current_milli_time()} - Message sent {m["MessageType"]}')
        if not repeat:
            break
