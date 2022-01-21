-- create stream to do change data update from snowpipe on table
create or replace stream "DE04"."VIDEO"."RAW_DATA_CDC"
on table "DE04"."VIDEO"."json_raw"
append_only=true
comment="stream on video raw json data from s3";


-- create stream to grab change data capture on table VIDEO_RAW
create or replace stream "DE04"."VIDEO"."VIDEO_RAW_STREAM"
on table "DE04"."VIDEO"."VIDEO_RAW"
append_only=true
comment="stream data on VIDEO_RAW table";


show streams;
select * from "DE04"."VIDEO"."RAW_DATA_CDC";