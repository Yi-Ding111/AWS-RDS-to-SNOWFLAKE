# AWS RDS MySQL to snowflake datapipeline

This project will use AWS services: RDS, Lambda, S3, IAM, Kinesis, SNS, SQS, SES, System Manager (parameter store), EventBridge (rule), CloudWatch and snowflake. 

This data pipeline can transfer data from RDS MySQL to snowflake automatically in schedule. Snowflake will trigger tasks to fill data into a star schema data warehouse.

> ## AWS services usage

Transaction records from some kinds Web services are stored in RDS MySQL. The database table structure can be seen in __create_rds_table.sql__.

Use System Manager (parameter store) to store the latest timestamp string of the last processed database data.

Lambda function __kinesis_producer(3).py__ determine the newly added data in the database according to the timestamp string stored in the parameter store, and extract the newly added part of the data into a defined format, then transforming them to kinesis data streams.

Add Cloudwatch logs (select corresponding log groups)as the trigger of lambda function __cloudwatch_error_lambda.py__. The lambda function will write log into SNS if it detect the new cloudwatch log has error response. 

It means that Lambda function __kinesis_producer(3).py__ did not extract records from RDS sucessfully if publishing message into SNS. SNS will push it into SQS (or can choose push message directly to trigger the other lambda function, but in case something wrong, SQS can keep events for long time) and trigger the lambda function __video_failure_ses.py__ to send email, telling the engineer that something is wrong and figure it. 

Kinesis Firehose will collect data from kinesis data stream and store it into S3 destination. 

Create EventBridge rule to Plan schedule for data extraction.

> ## SnowFlake usage

Create corresponding Dimension table, Fact table and staging table on snowflake,see details in __snowflake_raw flie__.

Create stream table on some specific tables, see in details in __video_raw_stream_CDC.sql__. In order to capture change data in stead of transforming all historic data each time.

Create task flows to do actions like data washing, data filling, data merging and etc., see details in __stream_task_ELT.sql__.

Create snowpipe __kinesis_s3_snowpipe__, give it corresponding IAM user credentials, (use channel notification) to connect corresponding S3 bucket. Extracting data automatically when S3 bucket has new input to destination. 

___
___

This project uses default VPC, just edit security group for RDS and lambda for testing.

___
___

Data from: JR academy

Created at: Oct 2021

> ## Contact
Author: Yi Ding

Email: dydifferent@gmail.com



