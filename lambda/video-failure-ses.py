#trigger SES if get SNS event to send email to tell somebody that video-kinesis-producer lambda is fail

import boto3
import json
import time 

ssm_client = boto3.client('ssm')
ses_client = boto3.client('ses')

def lambda_handler(event, context):

    #get the maximum timestamp value of the last database record processed
    ssm_response=ssm_client.get_parameter(Name='timestamp-check')
    last_time_timestamp=ssm_response['Parameter']['Value']

    #get lambda timestamp
    fail_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    email_body="""  
                    <br>
                    This is the lambda video-kinesis-producer alart:

                        lambda function is broken when dealing RDS records after timestamp {x} at local time {y}.
               """.format(x=last_time_timestamp,y=fail_time)
    

    message={
        "Subject":{
            "Data":"Lambda function alart"
        },
        "Body":{
            "Html":{
                "Data":email_body
            }
        }
    }

    ses_client.send_email(Source="dydifferent2@gmail.com",
                          Destination={"ToAddresses":["dydifferent2@gmail.com"]},
                          Message=message
                         )



    
