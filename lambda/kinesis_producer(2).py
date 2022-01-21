
import json
import pymysql
import os
import boto3
client = boto3.client('kinesis')
def lambda_handler(event, context):
    
    endpoint='videodb.cphi9pjahufa.ap-southeast-2.rds.amazonaws.com'
    username=os.environ['user']
    password=os.environ['password']
    databasename='video'
    
    
    connection=pymysql.connect(host=endpoint,user=username,passwd=password,db=databasename,port=3306)
    cursor=connection.cursor()
    cursor.execute('select * from video_init')

    rows=cursor.fetchall()
    
    for row in rows:
        payload={
            'DATETIME':row[0],
            'VIDEOTITLE':row[1],
            'EVENTS':row[2]
        }
        print("{0} {1} {2}".format(row[0],row[1],row[2]))
        response=client.put_record(
            StreamName='video_producer',
            Data=json.dumps(payload),
            PartitionKey='default'
            )
        print(response)
