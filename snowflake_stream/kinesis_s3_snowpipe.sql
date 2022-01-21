-- create json table to store json file
create or replace table "DE04"."VIDEO"."json_raw"(
json_data_raw VARIANT
);


-- create json file format
create or replace file format "DE04"."VIDEO"."raw_video_json_format"
type ='json'
strip_outer_array=true;

-- create external stage to grab data from s3
create or replace stage "DE04"."VIDEO"."json_external_s3_stage"
url='s3://video-csv-yiding/RDS-kinesis/'
credentials=(AWS_KEY_ID='AKIA2RHKGRLBTXZWUOXE',AWS_SECRET_KEY='*****************************');

show stages;


-- create snowpipe to auto get data from s3
create or replace pipe "DE04"."VIDEO"."json_external_s3_stage"
auto_ingest=true
as copy into "DE04"."VIDEO"."json_raw"
from @"DE04"."VIDEO"."json_external_s3_stage"
file_format="DE04"."VIDEO"."raw_video_json_format"
on_error='skip_file';
