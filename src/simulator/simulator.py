# export PYTHONPATH=.
import sys, os
sys.path.insert(0, os.getcwd())

from kafka import KafkaProducer
import json, time, argparse
from src.reader.reader import read_messages_files
import src.custom_logger.custom_logger as custom_logger

def current_milli_time():
    return round(time.time() * 1000)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", type=str, help="URL to Kafka Broker. Default localhost:9092", default="localhost:9092")
    parser.add_argument("--repeat", "-r", action='store_true', help="Loop on files")
    parser.add_argument("--fast", "-f", action="store_true", help="Double the speed")
    parser.add_argument("--debug", "-d", action="store_true", help="Debug mode")
    parser.add_argument("PATH", type=str, help="Path of files to read and push to Kafka")
    args = parser.parse_args()
    path = args.PATH
    bs = args.broker
    repeat = args.repeat
    debug = args.debug
    fast = args.fast

    if debug:
        logger = custom_logger.custom_logger(__name__, debug)
    else:
        logger = custom_logger.custom_logger(__name__)

    # Get list of messages
    messages = read_messages_files(path)

    # Get Kafka producer
    try:
        producer = KafkaProducer(bootstrap_servers=bs)
        logger.info(f"Connected to {bs}")
    except:
        logger.error(f'Not able to connect')
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
            sleep_time = sleep_time/2 if fast else sleep_time

            logger.info(f'Sleeping {sleep_time}s')
            time.sleep(sleep_time)
            previous_time = m_time # Move time forward based on current time

            # Produce message
            producer.send(topic='simulator', value=json.dumps(local_messsage).encode('UTF-8'))
            logger.info(f'Message {m["MessageType"]} sent')
            logger.debug(m)
        if repeat:
            logger.info('Repeating ...')
        else:
            logger.info('Completed')
            break
