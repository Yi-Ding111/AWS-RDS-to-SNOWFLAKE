import json
import boto3
import os
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (WriteRowsEvent)


def lambda_handler(event, context):
    # TODO implement
    kinesis = boto3.client("kinesis")
    
    #connect with RDS mysql
    stream = BinLogStreamReader(
        connection_settings= {
            "host": "videodb.cphi9pjahufa.ap-southeast-2.rds.amazonaws.com",
            "port": 3306,
            "user": os.environ['user'],
            "passwd": os.environ['password']
            },
          
        server_id=100,
        blocking=True,
        resume_stream=True,
        only_events=[WriteRowsEvent]
        
        )
    #read stream data into kinesis stream. 
    for binlogevent in stream:
        for row in binlogevent.rows:
            record = {"schema": binlogevent.schema,
            "table": binlogevent.table,
            "type": type(binlogevent).__name__,
            "row": row
            }
            
            kinesis.put_record(StreamName="video_producer", Data=json.dumps(record), PartitionKey="default")
            print (record)
