CREATE DATABASE IF NOT EXISTS ods;
USE ods;

CREATE EXTERNAL TABLE ods.bill_deviceinfo_offline_position(
	gps_lon DOUBLE COMMENT '经度'
	,gps_lat DOUBLE COMMENT '纬度'
	,export_ymd INT COMMENT '导出日期'
	,dt STRING COMMENT '业务日期'
)partitioned by (year string,month string,day string)
row format delimited fields terminated by ',';

LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1,partcol2=val2 ...)]
LOAD DATA INPATH '/data/geoc/2018/03/02/' OVERWRITE INTO TABLE ods.bill_deviceinfo_offline_position PARTITION (year=2018,month=03,day=02);







CREATE DATABASE IF NOT EXISTS dwd;
USE dwd;

CREATE TABLE IF NOT EXISTS dwd.dwd_bill_deviceinfo_offline_position_d(
	gps_lon DOUBLE COMMENT '经度'
	,gps_lat DOUBLE COMMENT '纬度'
	,export_ymd INT COMMENT '导出日期'
) COMMENT '坐标源数据'
partitioned by (pt_dt string)
STORED AS PARQUET;


INSERT OVERWRITE TABLE dwd.dwd_bill_deviceinfo_offline_position_d PARTITION(pt_dt = '2018-03-25')
SELECT gps_lon,gps_lat,export_ymd FROM ods.bill_deviceinfo_offline_position
WHERE year='2018' AND MONTH = '03' AND DAY = '02'