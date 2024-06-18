import argparse
import os

def read_sample_files(path: str):

    files = []
    for f in os.listdir(path=path):
        fileFullPath = os.path.join(path,f)
        if os.path.isfile(fileFullPath):
            files += [fileFullPath]

    return sorted(files)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("PATH",type=str,help="Path of files to read and push to Kafka")
    args = parser.parse_args()
    path = args.PATH

    print(read_sample_files(path))
    exit(0)