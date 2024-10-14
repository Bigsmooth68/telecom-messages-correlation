import argparse
import os
import json

def read_messages_files(path: str):
    """Read messages files from path

    Args:
        path (str): path to folder

    Returns:
        arr: array of messages
    """
    messages = []
    
    for f in sorted(os.listdir(path=path)):

        fileFullPath = os.path.join(path,f)

        if os.path.isfile(fileFullPath):
            
            with open(fileFullPath,mode="r") as file_content:
                messages.append(json.load(file_content))

    return messages

def read_messages_topic(topic_name: str):
    """Read messages from topic
    Args:
        topic_name (str): Topic name from which to read messages

    Returns:
        _type_: array of messages
    """
    return 0

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("PATH",type=str,help="Path of files to read messages from.")
    args = parser.parse_args()
    path = args.PATH

    print(read_messages_files(path))
    exit(0)