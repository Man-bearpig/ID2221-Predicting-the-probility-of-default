import argparse
from kafka import KafkaProducer
import pandas as pd
import json
import time
import datetime

KAFKA_BOOTSTRAP_SERVER="localhost:9092"

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', help='Input file')
    parser.add_argument('-c', '--chunksize', help='chunk size for big file')
    parser.add_argument('-s', '--sleeptime', help='sleep time in second')
    # parser.add_argument('-t', '--topic', help='kafka topic')
    args = parser.parse_args()
    '''
    Because the KPI file is big, we emulate by reading chunk, using iterator and chunksize
    '''
    INPUT_DATA_FILE=args.input_file
    chunksize=int(args.chunksize)
    sleeptime =int(args.sleeptime)
    KAFKA_TOPIC ="creditPrediction"
    '''
    we read data by chunk so we can handle a big sample data file
    '''
    # import os
    # print(os.getcwd()) #打印出当前工作路径
    input_data =pd.read_csv(INPUT_DATA_FILE,iterator=True,chunksize=chunksize)

    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

    for chunk_data in input_data:
        '''
        now process each chunk
        '''
        # chunk=chunk_data.dropna() 有缺失值会导致key不统一
        chunk = chunk_data
        for index, row in chunk.iterrows():
            '''
            Assume that when some data is available, we send it to Kafka in JSON
            '''
            # print(f'index:{index}, row: {row} \n')

            key = str(row.id).encode("utf-8")
            json_data = json.dumps(row.to_dict()).encode("utf-8")
            kafka_producer.send(topic=KAFKA_TOPIC,key=key,value=json_data)
            print(f'Send {json_data} to Kafka \n')

            # sleep a while, if needed as it is an emulation
            time.sleep(sleeptime)
