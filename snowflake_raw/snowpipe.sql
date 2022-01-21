-- log in snowflake server
snowsql -a *******.ap-southeast-2 -u yiding  

-- choose DB and create a new schema 
use DE04;
create schema video;
show schemas;

-- create raw data table
create or replace table video_raw ( 
"DATETIME" VARCHAR2(30), 
"VIDEOTITLE" VARCHAR2(200), 
"EVENTS" VARCHAR2(150)
);

-- create file format
create or replace file format "DE04"."VIDEO".raw_video_format
type = 'CSV'
compression = 'GZIP'
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
field_optionally_enclosed_by = '"'
trim_space=False
error_on_column_count_mismatch = true
escape = 'None'
date_format = 'AUTO'
null_if = ('NULL');


create or replace file format "DE04"."VIDEO".raw_video_format
type = 'csv'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY ='"';


-- create stage
create or replace stage "DE04"."VIDEO"."raw_video_state"
file_format = raw_video_format;

list @"DE04"."VIDEO"."raw_video_state";
ls @"DE04"."VIDEO"."raw_external_s3_stage";

put file:///Users/charles/Desktop/data_engineering/AWS_project/raw_data/video_data2.csv @"DE04"."VIDEO"."raw_video_state" auto_compress=true;

copy into VIDEO_RAW
from @"DE04"."VIDEO"."raw_video_state"/video_data2.csv.gz
file_format="DE04"."VIDEO".raw_video_format
on_error='skip_file';


-- Create an external s3 stage
create or replace stage "DE04"."VIDEO"."raw_external_s3_stage"
url='s3://video-csv-yiding/raw/'
credentials=(AWS_KEY_ID='AKIA2RHKGRLBTXZWUOXE',AWS_SECRET_KEY='*******************************');

show stages;

-- create snowpipe (copy data from external stage into table)
create or replace pipe "DE04"."VIDEO"."raw_external_s3_stage"
auto_ingest = true
as copy into "DE04"."VIDEO"."VIDEO_RAW"
from @"DE04"."VIDEO"."raw_external_s3_stage"
file_format=raw_video_format
on_error='skip_file';





-- connect SQS with ARN(snowpipe notification_channel) to trigger snowpipe
show pipes;


-- clean table 
delete from video_raw;








                        

