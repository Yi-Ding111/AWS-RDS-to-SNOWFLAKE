import os
import logging
import base64
import gzip
import boto3
import json


def lambda_handler(event, context):
   
    #get cloudwatch log
    cloudwatch_event=event["awslogs"]["data"]
    #decode
    decode_base64=base64.b64decode(cloudwatch_event)
    decompress_data=gzip.decompress(decode_base64)
    log_data=json.loads(decompress_data)
    
    print(log_data)
    
    info=log_data["logEvents"][1]["message"]
    
    #check if video-kinesis-producer lambda occurs error
    if "[ERROR]" in info:
       notification = "the lambda video-stream-producer has error."
       client = boto3.client('sns')
       response = client.publish (
          TargetArn = "arn:aws:sns:ap-southeast-2:724193151683:testsns",
          Message = json.dumps({'default': notification}),
          MessageStructure = 'json'
       )
      
      
   
       return {
          'statusCode': 400,
          'body': json.dumps(response)
       }