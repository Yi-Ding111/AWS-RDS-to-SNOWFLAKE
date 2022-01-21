create schema video;

use video;

drop table if exists video_init;

create table video_init (
	DATETIME VARCHAR(30), 
	VIDEOTITLE VARCHAR(200), 
	EVENTS VARCHAR(150),
	CREATE_TIME TIMESTAMP default now()                              
);

