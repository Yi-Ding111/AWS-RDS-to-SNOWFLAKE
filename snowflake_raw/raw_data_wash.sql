-- do data washing for raw_video table and inset data into videostart delta table
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
else 'UNKOWN' end as "SITE",
-- choose the last position as video info
TRIM(REGEXP_SUBSTR(VIDEOTITLE,'[^|]*$')) as "VIDEO"
from video_raw
where EVENTS like '%206%'
and regexp_count(VIDEOTITLE, '\|') !=0;

commit;