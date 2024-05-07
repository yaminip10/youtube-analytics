show databases 
use Youtube 
show tables 
create database YoutubeAnalytics;
------------------------------------------------------------
create
or replace TABLE.YOUTUBE_CHANNEL_STATS (
    CHANNELID VARCHAR(16777216),
    CHANNELTITLE VARCHAR(16777216),
    VIDEOID VARCHAR(16777216)
);
select *
from
    youtube_channel_stats;

drop table youtube_channel_stats;

select
    channeltitle,
    CHANNELID
from
    youtube_channel_stats
where
    VideoID = 'Kq-kQi6RaM8'
------------------------------------------------------------------------------------
create or replace TABLE YOUTUBEDATA.PUBLIC.YOUTUBE_VIDEO_STATS (
        VIDEOID VARCHAR(16777216) NOT NULL,
        VIEWCOUNT NUMBER(38, 0),
        LIKECOUNT NUMBER(38, 0),
        FAVORITECOUNT NUMBER(38, 0),
        COMMENTCOUNT NUMBER(38, 0),
        primary key (VIDEOID)
    );
    
select *
from
    YOUTUBE_VIDEO_STATS;
    
select
    distinct VIDEOID
from
    YOUTUBE_VIDEO_STATS;
    
drop table YOUTUBE_VIDEO_STATS;

select
    viewCount,
    likeCount,
    favoriteCount,
    commentCount
from
    YOUTUBE_VIDEO_STATS
where
    VIDEOID = 'b_35zbQP8tA';
-------------------------------------------------------------------------
    create
    or replace TABLE YOUTUBEDATA.PUBLIC.YOUTUBE_VIDEO_COMMENTS (
        VIDEO_ID VARCHAR(16777216),
        COMMENT_TEXT VARCHAR(16777216),
        unique (VIDEO_ID, COMMENT_TEXT)
    );

    
select *
from
    YOUTUBE_VIDEO_COMMENTS;
    
drop table YOUTUBE_VIDEO_COMMENTS;

select
    distinct video_id
from
    YOUTUBE_VIDEO_COMMENTS;






