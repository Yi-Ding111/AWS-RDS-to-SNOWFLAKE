-- read json format data into table
insert into "DE04"."VIDEO"."VIDEO_RAW"
select JSON_DATA_RAW:DATETIME, JSON_DATA_RAW:VIDEOTITLE, JSON_DATA_RAW:EVENTS
from "DE04"."VIDEO"."json_raw";//, lateral flatten(input =>JSON_DATA_RAW);

commit;

select * from "DE04"."VIDEO"."VIDEO_RAW";