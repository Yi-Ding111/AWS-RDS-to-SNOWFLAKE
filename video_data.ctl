OPTIONS (skip=1)
unrecoverable LOAD DATA
CHARACTERSET UTF8
INFILE './video_data.csv'
BADFILE './video_data.csv.bad'
DISCARDFILE './video_data.csv.dsc'
INTO TABLE VIDEOSTART_RAW
TRUNCATE
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
DateTime
,VideoTitle
,events
)