# export PYTHONPATH=.
import sys, os
sys.path.insert(0, os.getcwd())

from kafka import KafkaProducer
import json, time, argparse
from src.reader.reader import read_messages_files
import src.custom_logger.custom_logger as custom_logger
import random
import asyncio

def current_milli_time():
    return round(time.time() * 1000)

async def submit_message(message: str, sleep_time: int):
    # time.sleep(sleep_time)
    await asyncio.sleep(sleep_time)
    # Produce message
    producer.send(topic='simulator', value=json.dumps(message).encode('UTF-8'))
    logger.info(f'Message {message["MessageType"]} sent ({sleep_time})')
    logger.debug(message)

async def main():
    previous_time = 0
    try:
        tasks = []
        while True:
            if random_option:
                # generate fake OPC, DPC, CIC
                random_OPC = random.randrange(9999)
                random_DPC = random.randrange(9999)
                random_CIC = random.randrange(1000)

            for m in messages:
                local_messsage = m.copy()
                # Get message timestamp and update it with current time
                m_time = local_messsage['timestamp']
                local_messsage['timestamp'] = current_milli_time()

                if random_option:
                    local_messsage['OPC'] = random_OPC
                    local_messsage['DPC'] = random_DPC
                    local_messsage['CIC'] = random_CIC
                    random_time = random.randrange(0,2)
                else:
                    random_time = 0

                # Determine how much time we should wait between messages
                sleep_time = m_time - previous_time + random_time
                sleep_time = max(0, sleep_time) # Prevent negativ value on loop
                sleep_time = sleep_time/speed

                previous_time = m_time # Move time forward based on current time

                # Produce message
                logger.info('Send task')
                task = asyncio.create_task(submit_message(local_messsage, sleep_time))
                tasks.append(task)

            if repeat:
                logger.info('Repeating ...')
            else:
                logger.info('Completed')
                break
        await asyncio.gather(*tasks)
    finally:
        print('Exiting ...')


# End of functions

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", type=str, help="URL to Kafka Broker. Default localhost:9092", default="localhost:9092")
    parser.add_argument("--repeat", "-r", action='store_true', help="Loop on files")
    parser.add_argument("--fast", "-f", action="store_true", help="2x speed")
    parser.add_argument("--FAST", "-F", action="store_true", help="4x speed")
    parser.add_argument("--random", action="store_true", help="Replace OPC, DPC, CIC with random numbers and run at full speed.")
    parser.add_argument("--debug", "-d", action="store_true", help="Debug mode")
    parser.add_argument("PATH", type=str, help="Path of files to read and push to Kafka")
    args = parser.parse_args()
    path = args.PATH
    bs = args.broker
    repeat = args.repeat
    debug = args.debug
    fast = args.fast
    random_option = args.random

    if debug:
        logger = custom_logger.custom_logger(__name__, debug)
    else:
        logger = custom_logger.custom_logger(__name__)

    if args.FAST:
        speed = 4
    elif args.fast:
        speed = 2
    else:
        speed = 1

    # Get list of messages
    messages = read_messages_files(path)

    # Get Kafka producer
    try:
        producer = KafkaProducer(bootstrap_servers=bs)
        logger.info(f"Connected to {bs}")
    except:
        logger.error(f'Not able to connect')
        exit(1)

    asyncio.run(main())
    