import sys, os
sys.path.insert(0, os.getcwd())

from quixstreams import Application
import json
import src.protocol_keys.ISUP as isup

def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        consumer_group="correlator-quix"
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["simulator"])

        while True:
            msg = consumer.poll(1)
            if msg is None:
                print("Waiting ...")
            else:
                value = json.loads(msg.value())
                print(f"Processing {value}")
                closing = isup.process_message(value)
                if closing:
                    with app.get_producer() as producer:
                        producer.produce(
                            topic = "ISUP",
                            value = isup.close_xdr(value)
                        )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
