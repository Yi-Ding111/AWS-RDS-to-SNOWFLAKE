import json
import pymysql
import os
import boto3

kinesis_client = boto3.client('kinesis')
ssm_client = boto3.client('ssm')

def lambda_handler(event, context):
    
    #get the maximum timestamp value of the last database record processed
    ssm_response=ssm_client.get_parameter(Name='timestamp-check')
    last_time_timestamp=ssm_response['Parameter']['Value']
    
    #connect RDS mysql
    endpoint='videodb.cphi9pjahufa.ap-southeast-2.rds.amazonaws.com'
    username=os.environ['user']
    password=os.environ['password']
    databasename='video'
    
    RDS_mysql=pymysql.connect(host=endpoint,user=username,passwd=password,db=databasename,port=3306)
    
    
    #query
    cursor1=RDS_mysql.cursor()
    cursor1.execute('select * from video_init where CREATE_TIME>{x}'.format(x=last_time_timestamp))
    rows=cursor1.fetchall()
    
    
    #write query records in json format
    for row in rows:
        payload={
            'DATETIME':row[0],
            'VIDEOTITLE':row[1],
            'EVENTS':row[2]
        }
        #print("{0} {1} {2}".format(row[0],row[1],row[2]))
        
        #put records to kinesis data stream
        response=kinesis_client.put_record(
            StreamName='video_producer',
            Data=json.dumps(payload),
            PartitionKey='default'
            )
        #print(response)
    
    
    #update ssm parameter store value as the lasted timestamp in this records batch
    cursor2=RDS_mysql.cursor()
    cursor2.execute('SELECT MAX(CREATE_TIME) from video_init where CREATE_TIME>{x}'.format(x=last_time_timestamp))
    latest_timestamp=cursor2.fetchone()[0]
    timestamp_str=str("'")+str(latest_timestamp)+str("'")
    
    #write new parameter value to ssm
    ssm_client.put_parameter(
         Name='timestamp-check',
         Value=timestamp_str,
         Type='String',
         Overwrite=True
       )