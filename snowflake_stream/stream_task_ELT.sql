-- give permission to role to execute task

--change role to security admin
use role securityadmin;

-- administer snow tasks
create role snow_task_admin;

use role accountadmin;

-- give administer task permission
grant execute task on account to role snow_task_admin;

use role securityadmin;

-- give the task permission to our role
grant role snow_task_admin to role sysadmin;

use role sysadmin;



-- snow task to put json format data into raw table
create or replace task "DE04"."VIDEO"."MASTER_TASK"
warehouse=COMPUTE_WH
schedule='1 minute'
-- trigger task only when stream has data
when system$stream_has_data('DE04.VIDEO.RAW_DATA_CDC')
as
insert into "DE04"."VIDEO"."VIDEO_RAW"
select JSON_DATA_RAW:DATETIME, JSON_DATA_RAW:VIDEOTITLE, JSON_DATA_RAW:EVENTS
from "DE04"."VIDEO"."RAW_DATA_CDC";


-- change data in VIDEO_RAW wil be captured in stream






----------------------------------------------------- data wash and split info to destination delta table ---------------------------------------------

-- snow task to do data washing and put stream data into VIDEOSTART_DLT table
create or replace task "DE04"."VIDEO"."TO_VIDEOSTART_DLT"
warehouse=COMPUTE_WH
schedule='1 minute'
when system$stream_has_data('DE04.VIDEO.VIDEO_RAW_STREAM')
-- do data washing for raw_video table and inset data into videostart delta table
as 
insert into "DE04"."VIDEO"."VIDEOSTART_DLT" 
select TO_TIMESTAMP(DATETIME,'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as "DATETIME",
case when REGEXP_LIKE(upper(TRIM(REGEXP_SUBSTR(VIDEOTITLE,'[^|]+',1,1,'i'))), 'IPHONE')
then 'IPHONE'  
when REGEXP_LIKE(upper(TRIM(REGEXP_SUBSTR(VIDEOTITLE,'[^|]+',1,1,'i'))), 'APP')
then 'APP'
when REGEXP_LIKE(upper(TRIM(REGEXP_SUBSTR(VIDEOTITLE,'[^|]+',1,1,'i'))), 'ANDROID')
then 'ANDROID'
when REGEXP_LIKE(upper(TRIM(REGEXP_SUBSTR(VIDEOTITLE,'[^|]+',1,1,'i'))), 'NEWS' ) 
then 'DESKTOP'
else 'UNKNOWN' end as "PLATFORM",
case when REGEXP_LIKE(upper(TRIM(REGEXP_SUBSTR(VIDEOTITLE,'[^|]+',1,2,'i'))), 'LIVE.*')
then 'LIVE' 
else 'UNKONWN' end as "SITE",
TRIM(REGEXP_SUBSTR(VIDEOTITLE,'[^|]*$')) as "VIDEO"
from "DE04"."VIDEO"."VIDEO_RAW_STREAM"
where EVENTS like '%206%'
and regexp_count(VIDEOTITLE, '\|') !=0;


-- snow task to populate DIMDATE_DLT table
create or replace task "DE04"."VIDEO"."TO_DIMDATE_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TO_VIDEOSTART_DLT"
as 
insert into "DE04"."VIDEO"."DIMDATE_DLT"
select TO_CHAR(DATETIME,'YYYYMMDDHH24MI')
from "DE04"."VIDEO"."VIDEOSTART_DLT"
group by TO_CHAR(DATETIME,'YYYYMMDDHH24MI')
order by TO_CHAR(DATETIME,'YYYYMMDDHH24MI');


-- snow task to populate DIMPLATFORM_DLT table
create or replace task "DE04"."VIDEO"."TO_DIMPLATFORM_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TO_DIMDATE_DLT"
as
insert into "DE04"."VIDEO"."DIMPLATFORM_DLT"
select PLATFORM
from "DE04"."VIDEO"."VIDEOSTART_DLT"
group by PLATFORM
order by PLATFORM;


-- snow task to populate DIMSITE_DLT table
create or replace task "DE04"."VIDEO"."TO_DIMSITE_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TO_DIMPLATFORM_DLT"
as
insert into "DE04"."VIDEO"."DIMSITE_DLT"
select SITE
from "DE04"."VIDEO"."VIDEOSTART_DLT"
group by SITE
order by SITE;


-- snow task to populate DIMVIDEO_DLT table
create or replace task "DE04"."VIDEO"."TO_DIMVIDEO_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TO_DIMSITE_DLT"
as
insert into "DE04"."VIDEO"."DIMVIDEO_DLT"
select VIDEO
from "DE04"."VIDEO"."VIDEOSTART_DLT"
group by VIDEO
order by VIDEO;





---------------------------------------------------------- insert into dimension table ----------------------------------------------------

-- snow task to insert DIMPLATFORM table
create or replace task "DE04"."VIDEO"."INSERT_DIMPLATFORM"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TO_DIMVIDEO_DLT"
as
insert into DIMPLATFORM (PLATFORM)
select f.PLATFORM
from DIMPLATFORM_DLT f
left join DIMPLATFORM t
on f.PLATFORM = t.PLATFORM
where t.PLATFORM IS NULL;


-- snow task to insert DIMDATE table
create or replace task "DE04"."VIDEO"."INSERT_DIMDATE"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."INSERT_DIMPLATFORM"
as
insert into DIMDATE (DATETIME)
select f.DATETIME
from DIMDATE_DLT f
left join DIMDATE t
on f.DATETIME = t.DATETIME_SKEY
where t.DATETIME_SKEY IS NULL;


-- snow task to insert DIMSITE table
create or replace task "DE04"."VIDEO"."INSERT_DIMSITE"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."INSERT_DIMDATE"
as
insert into DIMSITE (SITE)
select f.SITE
from DIMSITE_DLT f
left join DIMSITE t
on f.SITE = t.SITE
where t.SITE IS NULL;


-- snow task to insert DIMVIDEO table
create or replace task "DE04"."VIDEO"."INSERT_DIMVIDEO"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."INSERT_DIMSITE"
as
insert into DIMVIDEO (VIDEO)
select f.VIDEO
from DIMVIDEO_DLT f
left join DIMVIDEO t
on f.VIDEO = t.VIDEO
where t.VIDEO IS NULL;


----------------------------------------------------------- insert into fact table -----------------------------------------------------------

-- snow task to insert fact table
create or replace task "DE04"."VIDEO"."INSERT_FACT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."INSERT_DIMVIDEO"
as
INSERT INTO FACTVIDEOSTART
SELECT DD.DATETIME_SKEY, DP.PLATFORM_SKEY, DS.SITE_SKEY, DV.VIDEO_SKEY, CURRENT_TIMESTAMP FROM VIDEOSTART_DLT VSD
LEFT JOIN DIMDATE DD ON TO_CHAR(VSD.DATETIME,'YYYYMMDDHH24MI') = DD.DATETIME
LEFT JOIN DIMPLATFORM DP ON VSD.PLATFORM = DP.PLATFORM
LEFT JOIN DIMSITE DS ON VSD.SITE = DS.SITE
LEFT JOIN DIMVIDEO DV ON VSD.VIDEO = DV.VIDEO;





-------------------------------------------------- truncate delta table before new append ---------------------------------------------

-- snow task truncate table VIDEOSTART_DLT
create or replace task "DE04"."VIDEO"."TRUNCATE_VIDEOSTART_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."INSERT_FACT"
as
truncate "DE04"."VIDEO"."VIDEOSTART_DLT";

-- snow task truncate table DIMDATE_DLT
create or replace task "DE04"."VIDEO"."TRUNCATE_DIMDATE_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TRUNCATE_VIDEOSTART_DLT"
as 
truncate table "DE04"."VIDEO"."DIMDATE_DLT";

-- snow task truncate table DIMPLATFORM_DLT
create or replace task "DE04"."VIDEO"."TRUNCATE_DIMPLATFORM_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TRUNCATE_DIMDATE_DLT"
as
truncate table "DE04"."VIDEO"."DIMPLATFORM_DLT";

-- snow task truncate table DIMSITE_DLT
create or replace task "DE04"."VIDEO"."TRUNCATE_DIMSITE_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TRUNCATE_DIMPLATFORM_DLT"
as
truncate table "DE04"."VIDEO"."DIMSITE_DLT";

-- snow task truncate table DIMVIDEO_DLT
create or replace task "DE04"."VIDEO"."TRUNCATE_DIMVIDEO_DLT"
warehouse=COMPUTE_WH
after "DE04"."VIDEO"."TRUNCATE_DIMSITE_DLT"
as
truncate table "DE04"."VIDEO"."DIMVIDEO_DLT";






show tasks;


select system$task_dependents_enable('DE04.VIDEO.MASTER_TASK');
select system$task_dependents_enable('DE04.VIDEO.TO_VIDEOSTART_DLT');

alter task "DE04"."VIDEO"."TO_VIDEOSTART_DLT" suspend;
alter task "DE04"."VIDEO"."MASTER_TASK" suspend;