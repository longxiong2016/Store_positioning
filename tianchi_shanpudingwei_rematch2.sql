--20171202 0.8578
------------------------------------------------------------------------------------------------------------------------get_data-----------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--获取源数据
CREATE TABLE IF NOT EXISTS  ccf_sl_shop_info AS
SELECT * FROM odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_shop_info;

CREATE TABLE IF NOT EXISTS ccf_sl_user_shop_behavior AS
SELECT * FROM odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_user_shop_behavior;

CREATE TABLE IF NOT EXISTS ccf_sl_test AS
SELECT * FROM odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_test;
------------------------------------------------------------------------------------------------------------------------data_preprocessing-------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--获取数据，进行处理
drop table if exists l_user_shop_behavior;
drop table if exists l_test;
drop table if exists l_shop_info;
--增加了distinct
create table if not exists l_user_shop_behavior as select distinct * from ccf_sl_user_shop_behavior;
create table if not exists l_test as select * from ccf_sl_test;
create table if not exists l_shop_info as select distinct * from ccf_sl_shop_info;

--wifi_infos变为多行
Drop table if exists user_shop_behavior1;
create table if not exists user_shop_behavior1 as
select  l_user_shop_behavior.*,
wifi_infos2 from l_user_shop_behavior 
lateral view explode(split(wifi_infos, ';')) myTable as wifi_infos2;
select * from user_shop_behavior1;

--wifi_infos分隔开id和signal和flag
Drop table if exists user_shop_behavior2;
create table if not exists user_shop_behavior2 as
select
      user_id,shop_id,to_date(time_stamp,'yyyy-mm-dd hh:mi') as time_stamp,longitude,latitude,wifi_infos,
	  split_part(wifi_infos2,'|',1) as wifi_bssid,cast(if(split_part(wifi_infos2,'|',2)='null',-43,split_part(wifi_infos2,'|',2)) as int) as wifi_signal,
	  if(split_part(wifi_infos2,'|',3)='true',1,0) as wifi_flag
from user_shop_behavior1;
select * from user_shop_behavior2;

--wifi多行变为同记录的一行计算平均强度和连接率
Drop table if exists l_user_shop_behavior_0;
create table if not exists l_user_shop_behavior_0 as
select 
      user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,
	  if(split_part(split_part(wifi_infos,';',1),'|',2)='null',1,0) as is_iphone,
	  avg(wifi_signal) as signal_avg,
	  avg(wifi_flag) as flag_avg
from user_shop_behavior2 
group by user_id,shop_id,time_stamp,longitude,latitude,wifi_infos;
select * from l_user_shop_behavior_0;

--排序
Drop table if exists user_shop_behavior3;
create table if not exists user_shop_behavior3 as
select
      user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,wifi_bssid,wifi_signal,wifi_flag,
	  row_number() over(partition by user_id,shop_id,time_stamp,longitude,latitude,wifi_infos order by wifi_signal desc) as signal_rank
from user_shop_behavior2;
select * from user_shop_behavior3;

--merge产生wifi1
Drop table if exists l_user_shop_behavior_1;
create table if not exists l_user_shop_behavior_1 as
select * from user_shop_behavior3 where signal_rank=1;

Drop table if exists user_shop_behavior3_1;
create table if not exists user_shop_behavior3_1 as
select a.*,b.wifi_bssid as wifi1_bssid,b.wifi_signal as wifi1_signal,b.wifi_flag as wifi1_flag from
l_user_shop_behavior_0 a left outer join l_user_shop_behavior_1 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;
select * from user_shop_behavior3_1;

--merge产生wifi2
Drop table if exists l_user_shop_behavior_2;
create table if not exists l_user_shop_behavior_2 as
select * from user_shop_behavior3 where signal_rank=2;

Drop table if exists user_shop_behavior3_2;
create table if not exists user_shop_behavior3_2 as
select a.*,b.wifi_bssid as wifi2_bssid,b.wifi_signal as wifi2_signal,b.wifi_flag as wifi2_flag from
user_shop_behavior3_1 a left outer join l_user_shop_behavior_2 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi3
Drop table if exists l_user_shop_behavior_3;
create table if not exists l_user_shop_behavior_3 as
select * from user_shop_behavior3 where signal_rank=3;

Drop table if exists user_shop_behavior3_3;
create table if not exists user_shop_behavior3_3 as
select a.*,b.wifi_bssid as wifi3_bssid,b.wifi_signal as wifi3_signal,b.wifi_flag as wifi3_flag from
user_shop_behavior3_2 a left outer join l_user_shop_behavior_3 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi4
Drop table if exists l_user_shop_behavior_4;
create table if not exists l_user_shop_behavior_4 as
select * from user_shop_behavior3 where signal_rank=4;

Drop table if exists user_shop_behavior3_4;
create table if not exists user_shop_behavior3_4 as
select a.*,b.wifi_bssid as wifi4_bssid,b.wifi_signal as wifi4_signal from
user_shop_behavior3_3 a left outer join l_user_shop_behavior_4 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi5
Drop table if exists l_user_shop_behavior_5;
create table if not exists l_user_shop_behavior_5 as
select * from user_shop_behavior3 where signal_rank=5;

Drop table if exists user_shop_behavior3_5;
create table if not exists user_shop_behavior3_5 as
select a.*,b.wifi_bssid as wifi5_bssid,b.wifi_signal as wifi5_signal from
user_shop_behavior3_4 a left outer join l_user_shop_behavior_5 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi6
Drop table if exists l_user_shop_behavior_6;
create table if not exists l_user_shop_behavior_6 as
select * from user_shop_behavior3 where signal_rank=6;

Drop table if exists user_shop_behavior3_6;
create table if not exists user_shop_behavior3_6 as
select a.*,b.wifi_bssid as wifi6_bssid,b.wifi_signal as wifi6_signal from
user_shop_behavior3_5 a left outer join l_user_shop_behavior_6 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi7
Drop table if exists l_user_shop_behavior_7;
create table if not exists l_user_shop_behavior_7 as
select * from user_shop_behavior3 where signal_rank=7;

Drop table if exists user_shop_behavior3_7;
create table if not exists user_shop_behavior3_7 as
select a.*,b.wifi_bssid as wifi7_bssid,b.wifi_signal as wifi7_signal from
user_shop_behavior3_6 a left outer join l_user_shop_behavior_7 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi8
Drop table if exists l_user_shop_behavior_8;
create table if not exists l_user_shop_behavior_8 as
select * from user_shop_behavior3 where signal_rank=8;

Drop table if exists user_shop_behavior3_8;
create table if not exists user_shop_behavior3_8 as
select a.*,b.wifi_bssid as wifi8_bssid,b.wifi_signal as wifi8_signal from
user_shop_behavior3_7 a left outer join l_user_shop_behavior_8 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi9
Drop table if exists l_user_shop_behavior_9;
create table if not exists l_user_shop_behavior_9 as
select * from user_shop_behavior3 where signal_rank=9;

Drop table if exists user_shop_behavior3_9;
create table if not exists user_shop_behavior3_9 as
select a.*,b.wifi_bssid as wifi9_bssid,b.wifi_signal as wifi9_signal from
user_shop_behavior3_8 a left outer join l_user_shop_behavior_9 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi10
Drop table if exists l_user_shop_behavior_10;
create table if not exists l_user_shop_behavior_10 as
select * from user_shop_behavior3 where signal_rank=10;

Drop table if exists user_shop_behavior4;
create table if not exists user_shop_behavior4 as
select a.*,b.wifi_bssid as wifi10_bssid,b.wifi_signal as wifi10_signal from
user_shop_behavior3_9 a left outer join l_user_shop_behavior_10 b
on a.user_id=b.user_id and a.shop_id=b.shop_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;
select * from user_shop_behavior4;

--初始划分训练集2017-08-25-----2017-08-31-------------------------------------------------------------------------------------------------------------------------------------------!!!!!!!!!!!!---------------------
Drop table if exists user_shop_behavior_train;
create table if not exists user_shop_behavior_train as
select distinct user_shop_behavior4.*
from user_shop_behavior4
where time_stamp >= to_date('2017-08-25 00:00','yyyy-mm-dd hh:mi') and wifi1_signal<100;--去除TMD大的异常值
select * from user_shop_behavior_train;

--初始测试集2017-09-01--------2017-09-14-------------------------------------------------------------------------------------------------------------------------------------------!!!!!!!!!!!!!!---------------
--wifi_infos变为多行
Drop table if exists l_test_1;
create table if not exists l_test_1 as
select  l_test.*, 
wifi_infos2 from l_test 
lateral view explode(split(wifi_infos, ';')) myTable as wifi_infos2;
select * from l_test_1;

--wifi_infos分隔开id和signal和flag
Drop table if exists l_test_2;
create table if not exists l_test_2 as
select
      user_id,row_id,mall_id,to_date(time_stamp,'yyyy-mm-dd hh:mi') as time_stamp,longitude,latitude,wifi_infos,
	  split_part(wifi_infos2,'|',1) as wifi_bssid,cast(if(split_part(wifi_infos2,'|',2)='null',-43,split_part(wifi_infos2,'|',2)) as int) as wifi_signal,
	  if(split_part(wifi_infos2,'|',3)='true',1,0) as wifi_flag
from l_test_1;
select * from l_test_2;

--wifi多行变为同记录的一行计算平均强度和连接率
Drop table if exists l_test0;
create table if not exists l_test0 as
select 
      user_id,row_id,mall_id,time_stamp,longitude,latitude,wifi_infos,
	  if(split_part(split_part(wifi_infos,';',1),'|',2)='null',1,0) as is_iphone,
	  avg(wifi_signal) as signal_avg,
	  avg(wifi_flag) as flag_avg
from l_test_2 
group by user_id,row_id,mall_id,time_stamp,longitude,latitude,wifi_infos;
select * from l_test0;

--排序
Drop table if exists l_test_3;
create table if not exists l_test_3 as
select
      user_id,row_id,mall_id,time_stamp,longitude,latitude,wifi_infos,wifi_bssid,wifi_signal,wifi_flag,
	  row_number() over(partition by user_id,row_id,mall_id,time_stamp,longitude,latitude,wifi_infos order by wifi_signal desc) as signal_rank
from l_test_2;
select * from l_test_3;

--merge产生wifi1
Drop table if exists l_t1;
create table if not exists l_t1 as
select * from l_test_3 where signal_rank=1;

Drop table if exists l_test_3_1;
create table if not exists l_test_3_1 as
select a.*,b.wifi_bssid as wifi1_bssid,b.wifi_signal as wifi1_signal,b.wifi_flag as wifi1_flag from
l_test0 a left outer join l_t1 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;
select * from l_test_3_1;

--merge产生wifi2
Drop table if exists l_t2;
create table if not exists l_t2 as
select * from l_test_3 where signal_rank=2;

Drop table if exists l_test_3_2;
create table if not exists l_test_3_2 as
select a.*,b.wifi_bssid as wifi2_bssid,b.wifi_signal as wifi2_signal,b.wifi_flag as wifi2_flag from
l_test_3_1 a left outer join l_t2 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi3
Drop table if exists l_t3;
create table if not exists l_t3 as
select * from l_test_3 where signal_rank=3;

Drop table if exists l_test_3_3;
create table if not exists l_test_3_3 as
select a.*,b.wifi_bssid as wifi3_bssid,b.wifi_signal as wifi3_signal,b.wifi_flag as wifi3_flag from
l_test_3_2 a left outer join l_t3 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi4
Drop table if exists l_t4;
create table if not exists l_t4 as
select * from l_test_3 where signal_rank=4;

Drop table if exists l_test_3_4;
create table if not exists l_test_3_4 as
select a.*,b.wifi_bssid as wifi4_bssid,b.wifi_signal as wifi4_signal from
l_test_3_3 a left outer join l_t4 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi5
Drop table if exists l_t5;
create table if not exists l_t5 as
select * from l_test_3 where signal_rank=5;

Drop table if exists l_test_3_5;
create table if not exists l_test_3_5 as
select a.*,b.wifi_bssid as wifi5_bssid,b.wifi_signal as wifi5_signal from
l_test_3_4 a left outer join l_t5 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi6
Drop table if exists l_t6;
create table if not exists l_t6 as
select * from l_test_3 where signal_rank=6;

Drop table if exists l_test_3_6;
create table if not exists l_test_3_6 as
select a.*,b.wifi_bssid as wifi6_bssid,b.wifi_signal as wifi6_signal from
l_test_3_5 a left outer join l_t6 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi7
Drop table if exists l_t7;
create table if not exists l_t7 as
select * from l_test_3 where signal_rank=7;

Drop table if exists l_test_3_7;
create table if not exists l_test_3_7 as
select a.*,b.wifi_bssid as wifi7_bssid,b.wifi_signal as wifi7_signal from
l_test_3_6 a left outer join l_t7 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi8
Drop table if exists l_t8;
create table if not exists l_t8 as
select * from l_test_3 where signal_rank=8;

Drop table if exists l_test_3_8;
create table if not exists l_test_3_8 as
select a.*,b.wifi_bssid as wifi8_bssid,b.wifi_signal as wifi8_signal from
l_test_3_7 a left outer join l_t8 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi9
Drop table if exists l_t9;
create table if not exists l_t9 as
select * from l_test_3 where signal_rank=9;

Drop table if exists l_test_3_9;
create table if not exists l_test_3_9 as
select a.*,b.wifi_bssid as wifi9_bssid,b.wifi_signal as wifi9_signal from
l_test_3_8 a left outer join l_t9 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;

--merge产生wifi10
Drop table if exists l_t10;
create table if not exists l_t10 as
select * from l_test_3 where signal_rank=10;

Drop table if exists l_test_test;
create table if not exists l_test_test as
select a.*,b.wifi_bssid as wifi10_bssid,b.wifi_signal as wifi10_signal from
l_test_3_9 a left outer join l_t10 b
on a.user_id=b.user_id and a.row_id=b.row_id and a.mall_id=b.mall_id and a.time_stamp=b.time_stamp and a.longitude=b.longitude and a.latitude=b.latitude and a.wifi_infos=b.wifi_infos;
select * from l_test_test;

-----------------------------------------------------------------------------------------------------------------------------------candidate_and_feature_extract----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--生成所有的wifi-shop匹配对l_shop_wifi
Drop table if exists l_shop_wifi;
create table if not exists l_shop_wifi as
select shop_id,wifi_bssid,wifi_signal,wifi_flag,time_stamp from user_shop_behavior2
where wifi_signal<20;--去除异常值
select * from l_shop_wifi;
-------------------------------------------------------------------------------train---------------------------------------------------
--训练集的候选集提取
--加入去重
Drop table if exists l_shop_wifi_train;
create table if not exists l_shop_wifi_train as
select distinct shop_id,wifi_bssid,wifi_signal,wifi_flag
from l_shop_wifi
where time_stamp < to_date('2017-08-25 00:00','yyyy-mm-dd hh:mi');-----------------------------------------------!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
select * from l_shop_wifi_train;

--特定wifi所对应的所有的shop强度排序  
Drop table if exists l_shop_wifi_train1;
create table if not exists l_shop_wifi_train1 as
select 
     wifi_bssid,wifi_signal,wifi_flag,shop_id,
     row_number() over(partition by wifi_bssid order by wifi_signal desc) as signal_rank
from l_shop_wifi_train;
select * from l_shop_wifi_train1;

--所有wifi_flag=1的均作为候选集
Drop table if exists l_shop_wifi_train2;
create table if not exists l_shop_wifi_train2 as
select wifi_bssid,shop_id from l_shop_wifi_train1
where wifi_flag = 1;
select * from l_shop_wifi_train2;

--取强度排前50的作为特定wifi的候选shop 
Drop table if exists l_shop_wifi_train3;
create table if not exists l_shop_wifi_train3 as
select wifi_bssid,shop_id from l_shop_wifi_train1
where signal_rank<51;

--得到最终的候选shop
Drop table if exists l_shop_wifi_train4;
create table if not exists l_shop_wifi_train4 as
select * from (select * from l_shop_wifi_train2 union select * from l_shop_wifi_train3) t;
select * from l_shop_wifi_train4;

--训练集user_shop_behavior_train先转为多行
Drop table if exists trainset;
create table if not exists trainset as
select  user_shop_behavior_train.*, wifi_infos2 from user_shop_behavior_train 
lateral view explode(split(wifi_infos, ';')) myTable as wifi_infos2;
select * from trainset;

Drop table if exists trainset1;
create table if not exists trainset1 as
select user_id,shop_id as shop_id_real,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,split_part(wifi_infos2,'|',1) as wifi_bssid from trainset;

--merge进候选集
Drop table if exists trainset2;
create table if not exists trainset2 as
select trainset1.*,l_shop_wifi_train4.shop_id from
trainset1 left outer join l_shop_wifi_train4
on trainset1.wifi_bssid=l_shop_wifi_train4.wifi_bssid;

--去重
Drop table if exists trainset3;
create table if not exists trainset3 as
select distinct user_id,shop_id,shop_id_real,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal from trainset2
where shop_id is not null;

--没有匹配到的记录用该mall的所有shop_id作为候选集
--原始集合
Drop table if exists trainset_label1;
create table if not exists trainset_label1 as
select user_id,shop_id as shop_id_real,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal from user_shop_behavior_train;
select * from trainset_label1;

--原始集合与候选集合进行merge,得到shop为null的行
Drop table if exists trainset4;
create table if not exists trainset4 as
select a.*,b.shop_id from
trainset_label1 a left outer join trainset3 b
on a.user_id=b.user_id and a.shop_id_real=b.shop_id_real and a.time_stamp=b.time_stamp and a.wifi_infos=b.wifi_infos;
select * from trainset4;

Drop table if exists trainset4_1;
create table if not exists trainset4_1 as
select * from trainset4
where shop_id is null;

Drop table if exists trainset4_2;
create table if not exists trainset4_2 as
select a.*,b.mall_id from
trainset4_1 a left outer join l_shop_info_last b
on a.shop_id_real=b.shop_id;

Drop table if exists trainset4_3;
create table if not exists trainset4_3 as
select a.user_id,b.shop_id,a.shop_id_real,a.time_stamp,a.longitude,a.latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal from
trainset4_2 a left outer join l_shop_info_last b
on a.mall_id=b.mall_id;

Drop table if exists trainset4_4;
create table if not exists trainset4_4 as
select * from (select * from trainset3 union select * from trainset4_3) t;
select * from trainset4_4;

--生成label
Drop table if exists trainset5;
create table if not exists trainset5 as
select user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,if(shop_id=shop_id_real,1,0) as label from trainset4_4;

--统计label为1的数量，用于计算覆盖率
select sum(label) from trainset5;
select count(*) from trainset5;

--生成原始集合即label为1的行
Drop table if exists trainset_label12;
create table if not exists trainset_label12 as
select user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,1 as label from user_shop_behavior_train;
select * from trainset_label12;

--原始集合与候选集合进行纵向连接，去重，得到最终的训练集（未加入交叉特征）
Drop table if exists trainsetall;
create table if not exists trainsetall as
select * from (select * from trainset_label12 union select * from trainset5) t;
select * from trainsetall;
-------------------------------------------------------------------------------------testset-------------------------------
--测试集的候选集提取（同时可用于shopwifi交叉特征提取）l_shop_wifi_test
--加入去重
Drop table if exists l_shop_wifi_test;
create table if not exists l_shop_wifi_test as
select distinct shop_id,wifi_bssid,wifi_signal,wifi_flag------------------------------------------------------------------------------!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
from l_shop_wifi;
select * from l_shop_wifi_test;

--特定wifi所对应的所有的shop强度排序  
Drop table if exists l_shop_wifi_test1;
create table if not exists l_shop_wifi_test1 as
select 
     wifi_bssid,wifi_signal,wifi_flag,shop_id,
     row_number() over(partition by wifi_bssid order by wifi_signal desc) as signal_rank
from l_shop_wifi_test;
select * from l_shop_wifi_test1;

--所有wifi_flag=1的均作为候选集
Drop table if exists l_shop_wifi_test2;
create table if not exists l_shop_wifi_test2 as
select wifi_bssid,shop_id from l_shop_wifi_test1
where wifi_flag = 1;
select * from l_shop_wifi_test2;

--取强度排前50的作为特定wifi的候选shop 
Drop table if exists l_shop_wifi_test3;
create table if not exists l_shop_wifi_test3 as
select wifi_bssid,shop_id from l_shop_wifi_test1
where signal_rank<51;

--得到最终的候选shop
Drop table if exists l_shop_wifi_test4;
create table if not exists l_shop_wifi_test4 as
select * from (select * from l_shop_wifi_test2 union select * from l_shop_wifi_test3) t;
select * from l_shop_wifi_test4;

--测试集l_test_test先转为多行
Drop table if exists testset;
create table if not exists testset as
select  l_test_test.*, wifi_infos2 from l_test_test 
lateral view explode(split(wifi_infos, ';')) myTable as wifi_infos2;
select * from testset;

Drop table if exists testset1;
create table if not exists testset1 as
select user_id,mall_id,row_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,split_part(wifi_infos2,'|',1) as wifi_bssid from testset;
select * from testset1;

--merge进候选集
Drop table if exists testset2;
create table if not exists testset2 as
select testset1.*,l_shop_wifi_test4.shop_id from
testset1 left outer join l_shop_wifi_test4
on testset1.wifi_bssid=l_shop_wifi_test4.wifi_bssid;
select * from testset2;

--去重
Drop table if exists testset3;
create table if not exists testset3 as
select distinct user_id,shop_id,mall_id,row_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal from testset2
where shop_id is not null;
select * from testset3;

--原始集合
Drop table if exists testset_label1;
create table if not exists testset_label1 as
select user_id,mall_id,row_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal from l_test_test;
select * from testset_label1;

--原始集合与候选集合进行merge,得到shop为null的行
Drop table if exists testset4;
create table if not exists testset4 as
select a.*,b.shop_id from
testset_label1 a left outer join testset3 b
on a.row_id=b.row_id;
select * from testset4;

--获取shop为null的行，找出这些行所在mall的所有shop作为候选集，替代之前方案填补-999
Drop table if exists testset4_1;
create table if not exists testset4_1 as
select * from testset4
where shop_id is null;

Drop table if exists testset4_2;
create table if not exists testset4_2 as
select a.user_id,a.mall_id,a.row_id,a.time_stamp,a.longitude,a.latitude,a.wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,
wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,
wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,b.shop_id from
testset4_1 a left outer join l_shop_info_last b
on a.mall_id=b.mall_id;

--纵向连接，得到最终的测试集（未加入交叉特征）
Drop table if exists testset_nonull;
create table if not exists testset_nonull as
select * from testset4
where shop_id is not null;

Drop table if exists testsetall;
create table if not exists testsetall as
select * from (select * from testset4_2 union select * from testset_nonull) t;
select * from testsetall;

------------------------------------------------------------------------------------------------------------------------------------------------------shop_feature_extract--------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------shop文件特征-----------------------------------------------
--处理shop_info文件，用于后续merge
Drop table if exists l_shop_info_last;
create table if not exists l_shop_info_last as
select
      shop_id,category_id,longitude as slongitude,latitude as slatitude,price,mall_id
from l_shop_info;
select * from l_shop_info_last;
-------------------------------------------------------------------------------------------------------------------------------------
--统计与shop有关的所有变量
Drop table if exists l_shop;
create table if not exists l_shop as
select shop_id,wifi_bssid,wifi_signal,wifi_flag,time_stamp,user_id,longitude,latitude,1 as a_transaction from user_shop_behavior2
where wifi_signal<20;--去除异常值
select * from l_shop;
-------------------------------------------------------------------------------train---------------------------------------------------
Drop table if exists l_shop_train;
create table if not exists l_shop_train as
select * from l_shop
where time_stamp < to_date('2017-08-25 00:00','yyyy-mm-dd hh:mi');-------------------------------------------------------------------------------------------!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
select * from l_shop_train;

--生成shop_wifi交叉特征，用于后续merge
Drop table if exists l_shop_train_wifi;
create table if not exists l_shop_train_wifi as
select 
      shop_id,wifi_bssid,avg(wifi_signal) as swifi_signal,avg(wifi_flag) as swifi_flag,a_transaction
from l_shop_train 
group by shop_id,wifi_bssid,a_transaction;
select * from l_shop_train_wifi;

Drop table if exists l_shop_train_wifi_allfeature;
create table if not exists l_shop_train_wifi_allfeature as
select 
      shop_id,wifi_bssid,avg(wifi_signal) as swifi_signal,avg(wifi_flag) as swifi_flag,sum(a_transaction)/55 as wifi_discover_lv,avg(longitude) as shop_wifi_longitude,avg(latitude) as shop_wifi_latitude
from l_shop_train 
group by shop_id,wifi_bssid;
select * from l_shop_train_wifi_allfeature;

--shop的所有wifi信号记录排名情况表
Drop table if exists all_shop_wifi_rank_train;
create table if not exists all_shop_wifi_rank_train as
select 
     shop_id,wifi_bssid,wifi_signal,wifi_flag,
     row_number() over(partition by shop_id order by wifi_signal desc) as signal_rank,
	 count(a_transaction) over(partition by shop_id ) as all_rank
from l_shop_train;
select * from all_shop_wifi_rank_train;

--生成shop的一般特征，用于后续merge进shop
Drop table if exists l_shop_train_info;
create table if not exists l_shop_train_info as
select 
      shop_id,avg(wifi_signal) as shop_signal_avg,avg(wifi_flag) as shop_flag_avg,avg(longitude) as shop_longitude_avg,
	  avg(latitude) as shop_latitude_avg,avg(datepart(time_stamp,'hour')) as shop_transaction_hour_avg,sum(a_transaction)/55 as shop_transaction_lv
from l_shop_train 
group by shop_id;
select * from l_shop_train_info;

--用前20%强的信号来获取另外一组shop特征
Drop table if exists l_shop_train_info1;
create table if not exists l_shop_train_info1 as
select 
      shop_id,avg(wifi_signal) as shop_signal_avg_20percent,avg(wifi_flag) as shop_flag_avg_20percent
from all_shop_wifi_rank_train 
where signal_rank <= all_rank*0.2
group by shop_id;
select * from l_shop_train_info1;

--生成shop 的 swifi_infos特征前10%-100%，用于后续merge
Drop table if exists shop_wifi_rank_train;
create table if not exists shop_wifi_rank_train as
select 
     shop_id,wifi_bssid,
     row_number() over(partition by shop_id order by swifi_signal desc) as signal_rank,
	 count(a_transaction) over(partition by shop_id ) as all_rank
from l_shop_train_wifi;
select * from shop_wifi_rank_train;

Drop table if exists shop_wifi_rank_train0;
create table if not exists shop_wifi_rank_train0 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos5
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.05
group by shop_id;
select * from shop_wifi_rank_train0;

Drop table if exists shop_wifi_rank_train1;
create table if not exists shop_wifi_rank_train1 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos10
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.1
group by shop_id;
select * from shop_wifi_rank_train1;

Drop table if exists shop_wifi_rank_train2;
create table if not exists shop_wifi_rank_train2 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos20
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.2
group by shop_id;

Drop table if exists shop_wifi_rank_train3;
create table if not exists shop_wifi_rank_train3 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos30
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.3
group by shop_id;

Drop table if exists shop_wifi_rank_train4;
create table if not exists shop_wifi_rank_train4 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos40
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.4
group by shop_id;

Drop table if exists shop_wifi_rank_train5;
create table if not exists shop_wifi_rank_train5 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos50
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.5
group by shop_id;

Drop table if exists shop_wifi_rank_train6;
create table if not exists shop_wifi_rank_train6 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos60
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.6
group by shop_id;

Drop table if exists shop_wifi_rank_train7;
create table if not exists shop_wifi_rank_train7 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos70
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.7
group by shop_id;

Drop table if exists shop_wifi_rank_train8;
create table if not exists shop_wifi_rank_train8 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos80
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.8
group by shop_id;

Drop table if exists shop_wifi_rank_train9;
create table if not exists shop_wifi_rank_train9 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos90
from shop_wifi_rank_train 
where signal_rank <= all_rank*0.9
group by shop_id;

Drop table if exists shop_wifi_rank_train10;
create table if not exists shop_wifi_rank_train10 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos100
from shop_wifi_rank_train 
group by shop_id;
select * from shop_wifi_rank_train10;

Drop table if exists l_shop_train_info_allfeature;
create table if not exists l_shop_train_info_allfeature as
select a.* ,b.shop_signal_avg_20percent,b.shop_flag_avg_20percent,c1.swifi_infos5,c.swifi_infos10,d.swifi_infos20,e.swifi_infos30,
f.swifi_infos40,g.swifi_infos50,h.swifi_infos60,i.swifi_infos70,j.swifi_infos80,k.swifi_infos90,l.swifi_infos100
from  l_shop_train_info a 
left outer join l_shop_train_info1 b on a.shop_id = b.shop_id
left outer join shop_wifi_rank_train0 c1 on a.shop_id = c1.shop_id
left outer join shop_wifi_rank_train1 c on a.shop_id = c.shop_id
left outer join shop_wifi_rank_train2 d on a.shop_id = d.shop_id
left outer join shop_wifi_rank_train3 e on a.shop_id = e.shop_id
left outer join shop_wifi_rank_train4 f on a.shop_id = f.shop_id
left outer join shop_wifi_rank_train5 g on a.shop_id = g.shop_id
left outer join shop_wifi_rank_train6 h on a.shop_id = h.shop_id
left outer join shop_wifi_rank_train7 i on a.shop_id = i.shop_id
left outer join shop_wifi_rank_train8 j on a.shop_id = j.shop_id
left outer join shop_wifi_rank_train9 k on a.shop_id = k.shop_id
left outer join shop_wifi_rank_train10 l on a.shop_id = l.shop_id;
select * from l_shop_train_info_allfeature;

--生成shop_user交叉特征，用于后续merge
Drop table if exists l_shop_train_user;
create table if not exists l_shop_train_user as
select 
      shop_id,user_id,sum(a_transaction)/55 as shop_user_transaction_lv,avg(datepart(time_stamp,'hour')) as shop_user_transaction_hour_avg,avg(longitude) as shop_user_longitude,avg(latitude) as shop_user_latitude,
	  avg(wifi_signal) as shop_user_wifisignal_avg
from l_shop_train 
group by shop_id,user_id;
select * from l_shop_train_user;

--添加新特征域

------------------------------------------------------------------------------test--------------------------------------------------------!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Drop table if exists l_shop_test;
create table if not exists l_shop_test as
select * from l_shop;

--生成shop_wifi交叉特征，用于后续merge
Drop table if exists l_shop_test_wifi;
create table if not exists l_shop_test_wifi as
select 
      shop_id,wifi_bssid,avg(wifi_signal) as swifi_signal,avg(wifi_flag) as swifi_flag,a_transaction
from l_shop_test 
group by shop_id,wifi_bssid,a_transaction;
select * from l_shop_test_wifi;

Drop table if exists l_shop_test_wifi_allfeature;
create table if not exists l_shop_test_wifi_allfeature as
select 
      shop_id,wifi_bssid,avg(wifi_signal) as swifi_signal,avg(wifi_flag) as swifi_flag,sum(a_transaction)/62 as wifi_discover_lv,avg(longitude) as shop_wifi_longitude,avg(latitude) as shop_wifi_latitude
from l_shop_test 
group by shop_id,wifi_bssid;

--shop的所有wifi信号记录排名情况表
Drop table if exists all_shop_wifi_rank_test;
create table if not exists all_shop_wifi_rank_test as
select 
     shop_id,wifi_bssid,wifi_signal,wifi_flag,
     row_number() over(partition by shop_id order by wifi_signal desc) as signal_rank,
	 count(a_transaction) over(partition by shop_id ) as all_rank
from l_shop_test;
select * from all_shop_wifi_rank_test;

--生成shop的一般特征，用于后续merge进shop
Drop table if exists l_shop_test_info;
create table if not exists l_shop_test_info as
select 
      shop_id,avg(wifi_signal) as shop_signal_avg,avg(wifi_flag) as shop_flag_avg,avg(longitude) as shop_longitude_avg,
	  avg(latitude) as shop_latitude_avg,avg(datepart(time_stamp,'hour')) as shop_transaction_hour_avg,sum(a_transaction)/62 as shop_transaction_lv
from l_shop_test 
group by shop_id;
select * from l_shop_test_info;

--用前20%强的信号来获取另外一组shop特征
Drop table if exists l_shop_test_info1;
create table if not exists l_shop_test_info1 as
select 
      shop_id,avg(wifi_signal) as shop_signal_avg_20percent,avg(wifi_flag) as shop_flag_avg_20percent
from all_shop_wifi_rank_test 
where signal_rank <= all_rank*0.2
group by shop_id;
select * from l_shop_test_info1;

--生成shop 的 swifi_infos特征前10%-100%，用于后续merge
Drop table if exists shop_wifi_rank_test;
create table if not exists shop_wifi_rank_test as
select 
     shop_id,wifi_bssid,
     row_number() over(partition by shop_id order by swifi_signal desc) as signal_rank,
	 count(a_transaction) over(partition by shop_id ) as all_rank
from l_shop_test_wifi;
select * from shop_wifi_rank_test;

Drop table if exists shop_wifi_rank_test0;
create table if not exists shop_wifi_rank_test0 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos5
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.05
group by shop_id;
select * from shop_wifi_rank_test0;

Drop table if exists shop_wifi_rank_test1;
create table if not exists shop_wifi_rank_test1 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos10
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.1
group by shop_id;

Drop table if exists shop_wifi_rank_test2;
create table if not exists shop_wifi_rank_test2 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos20
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.2
group by shop_id;

Drop table if exists shop_wifi_rank_test3;
create table if not exists shop_wifi_rank_test3 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos30
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.3
group by shop_id;

Drop table if exists shop_wifi_rank_test4;
create table if not exists shop_wifi_rank_test4 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos40
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.4
group by shop_id;

Drop table if exists shop_wifi_rank_test5;
create table if not exists shop_wifi_rank_test5 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos50
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.5
group by shop_id;

Drop table if exists shop_wifi_rank_test6;
create table if not exists shop_wifi_rank_test6 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos60
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.6
group by shop_id;

Drop table if exists shop_wifi_rank_test7;
create table if not exists shop_wifi_rank_test7 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos70
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.7
group by shop_id;

Drop table if exists shop_wifi_rank_test8;
create table if not exists shop_wifi_rank_test8 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos80
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.8
group by shop_id;

Drop table if exists shop_wifi_rank_test9;
create table if not exists shop_wifi_rank_test9 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos90
from shop_wifi_rank_test 
where signal_rank <= all_rank*0.9
group by shop_id;

Drop table if exists shop_wifi_rank_test10;
create table if not exists shop_wifi_rank_test10 as
select 
      shop_id,concat_ws('|',collect_set(wifi_bssid)) as swifi_infos100
from shop_wifi_rank_test 
group by shop_id;
select * from shop_wifi_rank_test10;

Drop table if exists l_shop_test_info_allfeature;
create table if not exists l_shop_test_info_allfeature as
select a.* ,b.shop_signal_avg_20percent,b.shop_flag_avg_20percent,c1.swifi_infos5,c.swifi_infos10,d.swifi_infos20,e.swifi_infos30,
f.swifi_infos40,g.swifi_infos50,h.swifi_infos60,i.swifi_infos70,j.swifi_infos80,k.swifi_infos90,l.swifi_infos100
from  l_shop_test_info a 
left outer join l_shop_test_info1 b on a.shop_id = b.shop_id
left outer join shop_wifi_rank_test0 c1 on a.shop_id = c1.shop_id
left outer join shop_wifi_rank_test1 c on a.shop_id = c.shop_id
left outer join shop_wifi_rank_test2 d on a.shop_id = d.shop_id
left outer join shop_wifi_rank_test3 e on a.shop_id = e.shop_id
left outer join shop_wifi_rank_test4 f on a.shop_id = f.shop_id
left outer join shop_wifi_rank_test5 g on a.shop_id = g.shop_id
left outer join shop_wifi_rank_test6 h on a.shop_id = h.shop_id
left outer join shop_wifi_rank_test7 i on a.shop_id = i.shop_id
left outer join shop_wifi_rank_test8 j on a.shop_id = j.shop_id
left outer join shop_wifi_rank_test9 k on a.shop_id = k.shop_id
left outer join shop_wifi_rank_test10 l on a.shop_id = l.shop_id;
select * from l_shop_test_info_allfeature;

--生成shop_user交叉特征，用于后续merge
Drop table if exists l_shop_test_user;
create table if not exists l_shop_test_user as
select 
      shop_id,user_id,sum(a_transaction)/62 as shop_user_transaction_lv,avg(datepart(time_stamp,'hour')) as shop_user_transaction_hour_avg,avg(longitude) as shop_user_longitude,avg(latitude) as shop_user_latitude,
	  avg(wifi_signal) as shop_user_wifisignal_avg
from l_shop_test
group by shop_id,user_id;
select * from l_shop_test_user;

--添加新特征域

--------------------------------------------------------------------------------------------------------------------------------------------------creat_trainset_and_testset--------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------trainset------------------------------------------------------
--merge shop特征
Drop table if exists trainsetall1_1;
create table if not exists trainsetall1_1 as
select a.*,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent,swifi_infos5,swifi_infos10,swifi_infos20,swifi_infos30,
swifi_infos40,swifi_infos50,swifi_infos60,swifi_infos70,swifi_infos80,swifi_infos90,swifi_infos100 from
trainsetall a left outer join l_shop_train_info_allfeature b
on a.shop_id=b.shop_id;

Drop table if exists trainsetall1_2;
create table if not exists trainsetall1_2 as
select trainsetall1_1.*,wifi_infos2 from trainsetall1_1 
lateral view explode(split(wifi_infos, ';')) myTable as wifi_infos2;

Drop table if exists trainsetall1_3;
create table if not exists trainsetall1_3 as
select  user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
if(swifi_infos5 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw5,if(swifi_infos10 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw10,
if(swifi_infos20 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw20,if(swifi_infos30 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw30,
if(swifi_infos40 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw40,if(swifi_infos50 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw50,
if(swifi_infos60 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw60,if(swifi_infos70 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw70,
if(swifi_infos80 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw80,if(swifi_infos90 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw90,
if(swifi_infos100 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw100,1 as all_num,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,
wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,label,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent
from trainsetall1_2;

Drop table if exists trainsetall1_4;
create table if not exists trainsetall1_4 as
select user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,
wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,label,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent,sum(all_num) as wifi_number,sum(wifi_in_sw5) as wifi_in_sw5_num,sum(wifi_in_sw5)/sum(all_num) as wifi_in_sw5_lv,
sum(wifi_in_sw10) as wifi_in_sw10_num,sum(wifi_in_sw10)/sum(all_num) as wifi_in_sw10_lv,sum(wifi_in_sw20) as wifi_in_sw20_num,sum(wifi_in_sw20)/sum(all_num) as wifi_in_sw20_lv,
sum(wifi_in_sw30) as wifi_in_sw30_num,sum(wifi_in_sw30)/sum(all_num) as wifi_in_sw30_lv,sum(wifi_in_sw40) as wifi_in_sw40_num,sum(wifi_in_sw40)/sum(all_num) as wifi_in_sw40_lv,
sum(wifi_in_sw50) as wifi_in_sw50_num,sum(wifi_in_sw50)/sum(all_num) as wifi_in_sw50_lv,sum(wifi_in_sw60) as wifi_in_sw60_num,sum(wifi_in_sw60)/sum(all_num) as wifi_in_sw60_lv,
sum(wifi_in_sw70) as wifi_in_sw70_num,sum(wifi_in_sw70)/sum(all_num) as wifi_in_sw70_lv,sum(wifi_in_sw80) as wifi_in_sw80_num,sum(wifi_in_sw80)/sum(all_num) as wifi_in_sw80_lv,
sum(wifi_in_sw90) as wifi_in_sw90_num,sum(wifi_in_sw90)/sum(all_num) as wifi_in_sw90_lv,sum(wifi_in_sw100) as wifi_in_sw100_num,sum(wifi_in_sw100)/sum(all_num) as wifi_in_sw100_lv
from trainsetall1_3 
group by user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,
wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,label,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent;
select * from trainsetall1_4;

--merge shop_wifi交叉特征
Drop table if exists trainsetall1;
create table if not exists trainsetall1 as
select a.*,b.swifi_signal as swifi1_signal,b.swifi_flag as swifi1_flag,b.wifi_discover_lv as wifi1_discover_lv,b.shop_wifi_longitude as shop_wifi1_longitude,b.shop_wifi_latitude as shop_wifi1_latitude,
c.swifi_signal as swifi2_signal,c.swifi_flag as swifi2_flag,c.wifi_discover_lv as wifi2_discover_lv,c.shop_wifi_longitude as shop_wifi2_longitude,c.shop_wifi_latitude as shop_wifi2_latitude,
d.swifi_signal as swifi3_signal,d.swifi_flag as swifi3_flag,d.wifi_discover_lv as wifi3_discover_lv,d.shop_wifi_longitude as shop_wifi3_longitude,d.shop_wifi_latitude as shop_wifi3_latitude,
e.swifi_signal as swifi4_signal,e.swifi_flag as swifi4_flag,e.wifi_discover_lv as wifi4_discover_lv,e.shop_wifi_longitude as shop_wifi4_longitude,e.shop_wifi_latitude as shop_wifi4_latitude,
f.swifi_signal as swifi5_signal,f.swifi_flag as swifi5_flag,f.wifi_discover_lv as wifi5_discover_lv,f.shop_wifi_longitude as shop_wifi5_longitude,f.shop_wifi_latitude as shop_wifi5_latitude,
g.swifi_signal as swifi6_signal,g.swifi_flag as swifi6_flag,g.wifi_discover_lv as wifi6_discover_lv,g.shop_wifi_longitude as shop_wifi6_longitude,g.shop_wifi_latitude as shop_wifi6_latitude,
h.swifi_signal as swifi7_signal,h.swifi_flag as swifi7_flag,h.wifi_discover_lv as wifi7_discover_lv,h.shop_wifi_longitude as shop_wifi7_longitude,h.shop_wifi_latitude as shop_wifi7_latitude,
i.swifi_signal as swifi8_signal,i.swifi_flag as swifi8_flag,i.wifi_discover_lv as wifi8_discover_lv,i.shop_wifi_longitude as shop_wifi8_longitude,i.shop_wifi_latitude as shop_wifi8_latitude,
j.swifi_signal as swifi9_signal,j.swifi_flag as swifi9_flag,j.wifi_discover_lv as wifi9_discover_lv,j.shop_wifi_longitude as shop_wifi9_longitude,j.shop_wifi_latitude as shop_wifi9_latitude,
k.swifi_signal as swifi10_signal,k.swifi_flag as swifi10_flag,k.wifi_discover_lv as wifi10_discover_lv,k.shop_wifi_longitude as shop_wifi10_longitude,k.shop_wifi_latitude as shop_wifi10_latitude
from trainsetall1_4 a 
left outer join l_shop_train_wifi_allfeature b on a.shop_id=b.shop_id and a.wifi1_bssid=b.wifi_bssid
left outer join l_shop_train_wifi_allfeature c on a.shop_id=c.shop_id and a.wifi2_bssid=c.wifi_bssid
left outer join l_shop_train_wifi_allfeature d on a.shop_id=d.shop_id and a.wifi3_bssid=d.wifi_bssid
left outer join l_shop_train_wifi_allfeature e on a.shop_id=e.shop_id and a.wifi4_bssid=e.wifi_bssid
left outer join l_shop_train_wifi_allfeature f on a.shop_id=f.shop_id and a.wifi5_bssid=f.wifi_bssid
left outer join l_shop_train_wifi_allfeature g on a.shop_id=g.shop_id and a.wifi6_bssid=g.wifi_bssid
left outer join l_shop_train_wifi_allfeature h on a.shop_id=h.shop_id and a.wifi7_bssid=h.wifi_bssid
left outer join l_shop_train_wifi_allfeature i on a.shop_id=i.shop_id and a.wifi8_bssid=i.wifi_bssid
left outer join l_shop_train_wifi_allfeature j on a.shop_id=j.shop_id and a.wifi9_bssid=j.wifi_bssid
left outer join l_shop_train_wifi_allfeature k on a.shop_id=k.shop_id and a.wifi10_bssid=k.wifi_bssid;
select * from trainsetall1;

--merge shop特征
Drop table if exists trainsetall2;
create table if not exists trainsetall2 as
select a.*,b.category_id,b.slongitude,b.slatitude,b.price,b.mall_id from
trainsetall1 a left outer join l_shop_info_last b
on a.shop_id=b.shop_id ;
--select * from trainsetall11;

Drop table if exists trainsetall3;
create table if not exists trainsetall3 as
select a.*,b.shop_user_transaction_lv,b.shop_user_transaction_hour_avg,b.shop_user_longitude,b.shop_user_latitude,b.shop_user_wifisignal_avg from
trainsetall2 a left outer join l_shop_train_user b
on a.shop_id=b.shop_id and a.user_id=b.user_id;
select * from trainsetall3;

--获得其他特征，同时将mall_id,shop_id等列进行类型转换int
Drop table if exists trainsetall4;
create table if not exists trainsetall4 as
select cast(split_part(user_id,'_',2) as int) as user_id,cast(split_part(shop_id,'_',2) as int) as shop_id,
datepart(time_stamp,'hour') as transaction_hour,weekday(time_stamp) as transaction_weekday,if(weekday(time_stamp)>4,1,0) as isoweekend,
case when datepart(time_stamp,'hour')>=6 and datepart(time_stamp,'hour')<10 then 1
     when datepart(time_stamp,'hour')>=10 and datepart(time_stamp,'hour')<14 then 2
	 when datepart(time_stamp,'hour')>=14 and datepart(time_stamp,'hour')<18 then 3
	 when datepart(time_stamp,'hour')>=18 and datepart(time_stamp,'hour')<22 then 4
	 when datepart(time_stamp,'hour')>=22 or datepart(time_stamp,'hour')<6 then 0 end as daytime_part,
longitude,latitude,is_iphone,signal_avg,flag_avg,cast(split_part(wifi10_bssid,'_',2) as int) as wifi10_bssid,
cast(split_part(wifi1_bssid,'_',2) as int) as wifi1_bssid,cast(split_part(wifi2_bssid,'_',2) as int) as wifi2_bssid,cast(split_part(wifi3_bssid,'_',2) as int) as wifi3_bssid,
cast(split_part(wifi4_bssid,'_',2) as int) as wifi4_bssid,cast(split_part(wifi5_bssid,'_',2) as int) as wifi5_bssid,cast(split_part(wifi6_bssid,'_',2) as int) as wifi6_bssid,
cast(split_part(wifi7_bssid,'_',2) as int) as wifi7_bssid,cast(split_part(wifi8_bssid,'_',2) as int) as wifi8_bssid,cast(split_part(wifi9_bssid,'_',2) as int) as wifi9_bssid,
wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,
wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,
wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,
swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,
swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,
swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,
swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,
swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,
cast(split_part(category_id,'_',2) as int) as category_id,cast(split_part(mall_id,'_',2) as int) as mall_id,slongitude,slatitude,price,
shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,
longitude-slongitude as longitudecha,latitude-slatitude as latitudecha,longitude-shop_longitude_avg as reallongitudecha,latitude-shop_latitude_avg as reallatitudecha,

wifi1_signal-swifi1_signal as wifi1_signalcha,wifi2_signal-swifi2_signal as wifi2_signalcha,
wifi3_signal-swifi3_signal as wifi3_signalcha,wifi4_signal-swifi4_signal as wifi4_signalcha,
wifi5_signal-swifi5_signal as wifi5_signalcha,wifi6_signal-swifi6_signal as wifi6_signalcha,
wifi7_signal-swifi7_signal as wifi7_signalcha,wifi8_signal-swifi8_signal as wifi8_signalcha,
wifi9_signal-swifi9_signal as wifi9_signalcha,wifi10_signal-swifi10_signal as wifi10_signalcha,

longitude-shop_wifi1_longitude as shop_wifi1_longitudecha,longitude-shop_wifi2_longitude as shop_wifi2_longitudecha,
longitude-shop_wifi3_longitude as shop_wifi3_longitudecha,longitude-shop_wifi4_longitude as shop_wifi4_longitudecha,
longitude-shop_wifi5_longitude as shop_wifi5_longitudecha,longitude-shop_wifi6_longitude as shop_wifi6_longitudecha,
longitude-shop_wifi7_longitude as shop_wifi7_longitudecha,longitude-shop_wifi8_longitude as shop_wifi8_longitudecha,
longitude-shop_wifi9_longitude as shop_wifi9_longitudecha,longitude-shop_wifi10_longitude as shop_wifi10_longitudecha,

latitude-shop_wifi1_latitude as shop_wifi1_latitudecha,latitude-shop_wifi2_latitude as shop_wifi2_latitudecha,
latitude-shop_wifi3_latitude as shop_wifi3_latitudecha,latitude-shop_wifi4_latitude as shop_wifi4_latitudecha,
latitude-shop_wifi5_latitude as shop_wifi5_latitudecha,latitude-shop_wifi6_latitude as shop_wifi6_latitudecha,
latitude-shop_wifi7_latitude as shop_wifi7_latitudecha,latitude-shop_wifi8_latitude as shop_wifi8_latitudecha,
latitude-shop_wifi9_latitude as shop_wifi9_latitudecha,latitude-shop_wifi10_latitude as shop_wifi10_latitudecha,

wifi_in_sw10_lv-wifi_in_sw5_lv as lv_5_10,wifi_in_sw20_lv-wifi_in_sw10_lv as lv_10_20,wifi_in_sw30_lv-wifi_in_sw20_lv as lv_20_30,
wifi_in_sw40_lv-wifi_in_sw30_lv as lv_30_40,wifi_in_sw50_lv-wifi_in_sw40_lv as lv_40_50,wifi_in_sw60_lv-wifi_in_sw50_lv as lv_50_60,
wifi_in_sw70_lv-wifi_in_sw60_lv as lv_60_70,wifi_in_sw80_lv-wifi_in_sw70_lv as lv_70_80,wifi_in_sw90_lv-wifi_in_sw80_lv as lv_80_90,
wifi_in_sw100_lv-wifi_in_sw90_lv as lv_90_100,

signal_avg-shop_signal_avg as shop_signal_avgcha,flag_avg-shop_flag_avg as shop_flag_avgcha,
wifi1_flag-swifi1_flag as wifi1_flagcha,wifi2_flag-swifi2_flag as wifi2_flagcha,wifi3_flag-swifi1_flag as wifi3_flagcha,
datepart(time_stamp,'hour')-shop_transaction_hour_avg as shop_transaction_hourcha,datepart(time_stamp,'hour')-shop_user_transaction_hour_avg as shop_user_transaction_hourcha,
longitude-shop_user_longitude as shop_user_longitudecha,latitude-shop_user_latitude as shop_user_latitudecha,signal_avg-shop_user_wifisignal_avg as shop_user_wifisignalcha,
label
from trainsetall3;
select * from trainsetall4;

Drop table if exists trainsetall5;
create table if not exists trainsetall5 as
select trainsetall4.*,longitudecha*longitudecha+latitudecha*latitudecha as longlat2,reallongitudecha*reallongitudecha+reallatitudecha*reallatitudecha as reallonglat2 from trainsetall4;
select * from trainsetall5;
----------------------------------------------------------------testset------------------------------------------------------
--merge shop特征
Drop table if exists testsetall1_1;
create table if not exists testsetall1_1 as
select a.*,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent,swifi_infos5,swifi_infos10,swifi_infos20,swifi_infos30,
swifi_infos40,swifi_infos50,swifi_infos60,swifi_infos70,swifi_infos80,swifi_infos90,swifi_infos100 from
testsetall a left outer join l_shop_test_info_allfeature b
on a.shop_id=b.shop_id;

Drop table if exists testsetall1_2;
create table if not exists testsetall1_2 as
select testsetall1_1.*,wifi_infos2 from testsetall1_1 
lateral view explode(split(wifi_infos, ';')) myTable as wifi_infos2;

Drop table if exists testsetall1_3;
create table if not exists testsetall1_3 as
select  user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
if(swifi_infos5 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw5,if(swifi_infos10 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw10,
if(swifi_infos20 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw20,if(swifi_infos30 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw30,
if(swifi_infos40 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw40,if(swifi_infos50 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw50,
if(swifi_infos60 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw60,if(swifi_infos70 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw70,
if(swifi_infos80 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw80,if(swifi_infos90 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw90,
if(swifi_infos100 rlike split_part(wifi_infos2,'|',1),1,0) as wifi_in_sw100,1 as all_num,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,
wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,row_id,mall_id,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent
from testsetall1_2;

Drop table if exists testsetall1_4;
create table if not exists testsetall1_4 as
select user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,
wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,row_id,mall_id,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent,sum(all_num) as wifi_number,sum(wifi_in_sw5) as wifi_in_sw5_num,sum(wifi_in_sw5)/sum(all_num) as wifi_in_sw5_lv,
sum(wifi_in_sw10) as wifi_in_sw10_num,sum(wifi_in_sw10)/sum(all_num) as wifi_in_sw10_lv,sum(wifi_in_sw20) as wifi_in_sw20_num,sum(wifi_in_sw20)/sum(all_num) as wifi_in_sw20_lv,
sum(wifi_in_sw30) as wifi_in_sw30_num,sum(wifi_in_sw30)/sum(all_num) as wifi_in_sw30_lv,sum(wifi_in_sw40) as wifi_in_sw40_num,sum(wifi_in_sw40)/sum(all_num) as wifi_in_sw40_lv,
sum(wifi_in_sw50) as wifi_in_sw50_num,sum(wifi_in_sw50)/sum(all_num) as wifi_in_sw50_lv,sum(wifi_in_sw60) as wifi_in_sw60_num,sum(wifi_in_sw60)/sum(all_num) as wifi_in_sw60_lv,
sum(wifi_in_sw70) as wifi_in_sw70_num,sum(wifi_in_sw70)/sum(all_num) as wifi_in_sw70_lv,sum(wifi_in_sw80) as wifi_in_sw80_num,sum(wifi_in_sw80)/sum(all_num) as wifi_in_sw80_lv,
sum(wifi_in_sw90) as wifi_in_sw90_num,sum(wifi_in_sw90)/sum(all_num) as wifi_in_sw90_lv,sum(wifi_in_sw100) as wifi_in_sw100_num,sum(wifi_in_sw100)/sum(all_num) as wifi_in_sw100_lv
from testsetall1_3 
group by user_id,shop_id,time_stamp,longitude,latitude,wifi_infos,is_iphone,signal_avg,flag_avg,
wifi1_bssid,wifi1_signal,wifi1_flag,wifi2_bssid,wifi2_signal,wifi2_flag,wifi3_bssid,wifi3_signal,wifi3_flag,wifi4_bssid,wifi4_signal,wifi5_bssid,wifi5_signal,
wifi6_bssid,wifi6_signal,wifi7_bssid,wifi7_signal,wifi8_bssid,wifi8_signal,wifi9_bssid,wifi9_signal,wifi10_bssid,wifi10_signal,row_id,mall_id,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,
shop_signal_avg_20percent,shop_flag_avg_20percent;
select * from testsetall1_4;

--merge shop_wifi交叉特征
Drop table if exists testsetall1;
create table if not exists testsetall1 as
select a.*,b.swifi_signal as swifi1_signal,b.swifi_flag as swifi1_flag,b.wifi_discover_lv as wifi1_discover_lv,b.shop_wifi_longitude as shop_wifi1_longitude,b.shop_wifi_latitude as shop_wifi1_latitude,
c.swifi_signal as swifi2_signal,c.swifi_flag as swifi2_flag,c.wifi_discover_lv as wifi2_discover_lv,c.shop_wifi_longitude as shop_wifi2_longitude,c.shop_wifi_latitude as shop_wifi2_latitude,
d.swifi_signal as swifi3_signal,d.swifi_flag as swifi3_flag,d.wifi_discover_lv as wifi3_discover_lv,d.shop_wifi_longitude as shop_wifi3_longitude,d.shop_wifi_latitude as shop_wifi3_latitude,
e.swifi_signal as swifi4_signal,e.swifi_flag as swifi4_flag,e.wifi_discover_lv as wifi4_discover_lv,e.shop_wifi_longitude as shop_wifi4_longitude,e.shop_wifi_latitude as shop_wifi4_latitude,
f.swifi_signal as swifi5_signal,f.swifi_flag as swifi5_flag,f.wifi_discover_lv as wifi5_discover_lv,f.shop_wifi_longitude as shop_wifi5_longitude,f.shop_wifi_latitude as shop_wifi5_latitude,
g.swifi_signal as swifi6_signal,g.swifi_flag as swifi6_flag,g.wifi_discover_lv as wifi6_discover_lv,g.shop_wifi_longitude as shop_wifi6_longitude,g.shop_wifi_latitude as shop_wifi6_latitude,
h.swifi_signal as swifi7_signal,h.swifi_flag as swifi7_flag,h.wifi_discover_lv as wifi7_discover_lv,h.shop_wifi_longitude as shop_wifi7_longitude,h.shop_wifi_latitude as shop_wifi7_latitude,
i.swifi_signal as swifi8_signal,i.swifi_flag as swifi8_flag,i.wifi_discover_lv as wifi8_discover_lv,i.shop_wifi_longitude as shop_wifi8_longitude,i.shop_wifi_latitude as shop_wifi8_latitude,
j.swifi_signal as swifi9_signal,j.swifi_flag as swifi9_flag,j.wifi_discover_lv as wifi9_discover_lv,j.shop_wifi_longitude as shop_wifi9_longitude,j.shop_wifi_latitude as shop_wifi9_latitude,
k.swifi_signal as swifi10_signal,k.swifi_flag as swifi10_flag,k.wifi_discover_lv as wifi10_discover_lv,k.shop_wifi_longitude as shop_wifi10_longitude,k.shop_wifi_latitude as shop_wifi10_latitude
from testsetall1_4 a 
left outer join l_shop_test_wifi_allfeature b on a.shop_id=b.shop_id and a.wifi1_bssid=b.wifi_bssid
left outer join l_shop_test_wifi_allfeature c on a.shop_id=c.shop_id and a.wifi2_bssid=c.wifi_bssid
left outer join l_shop_test_wifi_allfeature d on a.shop_id=d.shop_id and a.wifi3_bssid=d.wifi_bssid
left outer join l_shop_test_wifi_allfeature e on a.shop_id=e.shop_id and a.wifi4_bssid=e.wifi_bssid
left outer join l_shop_test_wifi_allfeature f on a.shop_id=f.shop_id and a.wifi5_bssid=f.wifi_bssid
left outer join l_shop_test_wifi_allfeature g on a.shop_id=g.shop_id and a.wifi6_bssid=g.wifi_bssid
left outer join l_shop_test_wifi_allfeature h on a.shop_id=h.shop_id and a.wifi7_bssid=h.wifi_bssid
left outer join l_shop_test_wifi_allfeature i on a.shop_id=i.shop_id and a.wifi8_bssid=i.wifi_bssid
left outer join l_shop_test_wifi_allfeature j on a.shop_id=j.shop_id and a.wifi9_bssid=j.wifi_bssid
left outer join l_shop_test_wifi_allfeature k on a.shop_id=k.shop_id and a.wifi10_bssid=k.wifi_bssid;
select * from testsetall1;

--merge shop特征
Drop table if exists testsetall2;
create table if not exists testsetall2 as
select a.*,b.category_id,b.slongitude,b.slatitude,b.price from
testsetall1 a left outer join l_shop_info_last b
on a.shop_id=b.shop_id ;

Drop table if exists testsetall3;
create table if not exists testsetall3 as
select a.*,b.shop_user_transaction_lv,b.shop_user_transaction_hour_avg,b.shop_user_longitude,b.shop_user_latitude,b.shop_user_wifisignal_avg from
testsetall2 a left outer join l_shop_test_user b
on a.shop_id=b.shop_id and a.user_id=b.user_id;
select * from testsetall3;

--获得其他特征，同时将mall_id,shop_id等列进行类型转换int
Drop table if exists testsetall4;
create table if not exists testsetall4 as
select cast(split_part(user_id,'_',2) as int) as user_id,cast(split_part(shop_id,'_',2) as int) as shop_id,
datepart(time_stamp,'hour') as transaction_hour,weekday(time_stamp) as transaction_weekday,if(weekday(time_stamp)>4,1,0) as isoweekend,
case when datepart(time_stamp,'hour')>=6 and datepart(time_stamp,'hour')<10 then 1
     when datepart(time_stamp,'hour')>=10 and datepart(time_stamp,'hour')<14 then 2
	 when datepart(time_stamp,'hour')>=14 and datepart(time_stamp,'hour')<18 then 3
	 when datepart(time_stamp,'hour')>=18 and datepart(time_stamp,'hour')<22 then 4
	 when datepart(time_stamp,'hour')>=22 or datepart(time_stamp,'hour')<6 then 0 end as daytime_part,
longitude,latitude,is_iphone,signal_avg,flag_avg,cast(split_part(wifi10_bssid,'_',2) as int) as wifi10_bssid,
cast(split_part(wifi1_bssid,'_',2) as int) as wifi1_bssid,cast(split_part(wifi2_bssid,'_',2) as int) as wifi2_bssid,cast(split_part(wifi3_bssid,'_',2) as int) as wifi3_bssid,
cast(split_part(wifi4_bssid,'_',2) as int) as wifi4_bssid,cast(split_part(wifi5_bssid,'_',2) as int) as wifi5_bssid,cast(split_part(wifi6_bssid,'_',2) as int) as wifi6_bssid,
cast(split_part(wifi7_bssid,'_',2) as int) as wifi7_bssid,cast(split_part(wifi8_bssid,'_',2) as int) as wifi8_bssid,cast(split_part(wifi9_bssid,'_',2) as int) as wifi9_bssid,
wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,
shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,
wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,
wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,
swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,
swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,
swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,
swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,
swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,
cast(split_part(category_id,'_',2) as int) as category_id,cast(split_part(mall_id,'_',2) as int) as mall_id,slongitude,slatitude,price,
shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,
longitude-slongitude as longitudecha,latitude-slatitude as latitudecha,longitude-shop_longitude_avg as reallongitudecha,latitude-shop_latitude_avg as reallatitudecha,

wifi1_signal-swifi1_signal as wifi1_signalcha,wifi2_signal-swifi2_signal as wifi2_signalcha,
wifi3_signal-swifi3_signal as wifi3_signalcha,wifi4_signal-swifi4_signal as wifi4_signalcha,
wifi5_signal-swifi5_signal as wifi5_signalcha,wifi6_signal-swifi6_signal as wifi6_signalcha,
wifi7_signal-swifi7_signal as wifi7_signalcha,wifi8_signal-swifi8_signal as wifi8_signalcha,
wifi9_signal-swifi9_signal as wifi9_signalcha,wifi10_signal-swifi10_signal as wifi10_signalcha,

longitude-shop_wifi1_longitude as shop_wifi1_longitudecha,longitude-shop_wifi2_longitude as shop_wifi2_longitudecha,
longitude-shop_wifi3_longitude as shop_wifi3_longitudecha,longitude-shop_wifi4_longitude as shop_wifi4_longitudecha,
longitude-shop_wifi5_longitude as shop_wifi5_longitudecha,longitude-shop_wifi6_longitude as shop_wifi6_longitudecha,
longitude-shop_wifi7_longitude as shop_wifi7_longitudecha,longitude-shop_wifi8_longitude as shop_wifi8_longitudecha,
longitude-shop_wifi9_longitude as shop_wifi9_longitudecha,longitude-shop_wifi10_longitude as shop_wifi10_longitudecha,

latitude-shop_wifi1_latitude as shop_wifi1_latitudecha,latitude-shop_wifi2_latitude as shop_wifi2_latitudecha,
latitude-shop_wifi3_latitude as shop_wifi3_latitudecha,latitude-shop_wifi4_latitude as shop_wifi4_latitudecha,
latitude-shop_wifi5_latitude as shop_wifi5_latitudecha,latitude-shop_wifi6_latitude as shop_wifi6_latitudecha,
latitude-shop_wifi7_latitude as shop_wifi7_latitudecha,latitude-shop_wifi8_latitude as shop_wifi8_latitudecha,
latitude-shop_wifi9_latitude as shop_wifi9_latitudecha,latitude-shop_wifi10_latitude as shop_wifi10_latitudecha,

wifi_in_sw10_lv-wifi_in_sw5_lv as lv_5_10,wifi_in_sw20_lv-wifi_in_sw10_lv as lv_10_20,wifi_in_sw30_lv-wifi_in_sw20_lv as lv_20_30,
wifi_in_sw40_lv-wifi_in_sw30_lv as lv_30_40,wifi_in_sw50_lv-wifi_in_sw40_lv as lv_40_50,wifi_in_sw60_lv-wifi_in_sw50_lv as lv_50_60,
wifi_in_sw70_lv-wifi_in_sw60_lv as lv_60_70,wifi_in_sw80_lv-wifi_in_sw70_lv as lv_70_80,wifi_in_sw90_lv-wifi_in_sw80_lv as lv_80_90,
wifi_in_sw100_lv-wifi_in_sw90_lv as lv_90_100,

signal_avg-shop_signal_avg as shop_signal_avgcha,flag_avg-shop_flag_avg as shop_flag_avgcha,
wifi1_flag-swifi1_flag as wifi1_flagcha,wifi2_flag-swifi2_flag as wifi2_flagcha,wifi3_flag-swifi1_flag as wifi3_flagcha,
datepart(time_stamp,'hour')-shop_transaction_hour_avg as shop_transaction_hourcha,datepart(time_stamp,'hour')-shop_user_transaction_hour_avg as shop_user_transaction_hourcha,
longitude-shop_user_longitude as shop_user_longitudecha,latitude-shop_user_latitude as shop_user_latitudecha,signal_avg-shop_user_wifisignal_avg as shop_user_wifisignalcha,
row_id
from testsetall3;
select * from testsetall4;

Drop table if exists testsetall5;
create table if not exists testsetall5 as
select testsetall4.*,longitudecha*longitudecha+latitudecha*latitudecha as longlat2,reallongitudecha*reallongitudecha+reallatitudecha*reallatitudecha as reallonglat2 from testsetall4;
select * from testsetall5;
----------------------------------------------------------------------------------------------------------------------------------------------------fill_missing_data----------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--由于表过大，直接在在PAI平台实现方便
-------------------------------------------------------------------------------------------------------------------------------------------------------xgb_long-----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--xgb模型训练，输入训练集long_xgb_train，测试集long_xgb_test，训练集和测试集生成通过机器学习平台填补缺失值后生成，xgb预测出long_xgb_pred；
drop table if exists long_xgb_pred;
DROP OFFLINEMODEL IF EXISTS long_xgboost_1;

-- train
PAI
-name xgboost
-project algo_public
-Deta="0.08"
-Dobjective="binary:logistic"
-DitemDelimiter=","
-Dseed="0"
-Dnum_round="4000"
-DlabelColName="label"
-DinputTableName="long_xgb_train"
-DenableSparse="false"
-Dmax_depth="8"
-Dsubsample="0.8"
-Dcolsample_bytree="0.8"
-DmodelName="long_xgboost_1"
-Dgamma="0"
-Dlambda="50" 
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,longitudecha,latitudecha,reallongitudecha,reallatitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_transaction_hourcha,shop_user_longitudecha,shop_user_latitudecha,shop_user_wifisignalcha,longlat2,reallonglat2"
-Dbase_score="0.11"
-Dmin_child_weight="100"
-DkvDelimiter=":";

-- predict
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="row_id,shop_id"
-DmodelName="long_xgboost_1"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="long_xgb_pred"
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,longitudecha,latitudecha,reallongitudecha,reallatitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_transaction_hourcha,shop_user_longitudecha,shop_user_latitudecha,shop_user_wifisignalcha,longlat2,reallonglat2"
-DinputTableName="long_xgb_test"
-DenableSparse="false";

----------------------------------------------------------------------------------------------------------------------------------------------------------submission_long--------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--对模型生成的概率结果进行处理，得到预测结果为1的概率
Drop table if exists long_result1;
create table long_result1 as select row_id,shop_id,
case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability from long_xgb_pred;
select * from long_result1;

--对概率从大到小进行排序
Drop table if exists l_result;
create table if not exists l_result as
select 
     row_id,shop_id,probability,
     row_number() over(partition by row_id order by probability desc) as prob_rank
from long_result1;
select * from l_result;

--获取概率最大所对应的shop_id作为预测结果
Drop table if exists long_submission_10;
create table if not exists long_submission_10 as
select 
      row_id,
      concat('s_',shop_id) as shop_id
from l_result
where prob_rank=1;
select * from long_submission_10;
select count(distinct row_id) from long_submission_10;

--提交结果
Drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict as
select * from long_submission_10;
select * from ant_tianchi_ccf_sl_predict;

--保存概率文件
Drop table if exists long_xgb_pred_10_20171202;
create table if not exists long_xgb_pred_10_20171202 as
select * from long_xgb_pred;
select * from long_xgb_pred_10_20171202;

--保存概率为1的文件
Drop table if exists long_xgb_pred111_10_20171202;
create table if not exists long_xgb_pred111_10_20171202 as
select * from long_result1;

------------------------------------------------------------------------------------ps---------------------------------------------------------------------------------------------
--对模型生成的概率结果进行处理，得到预测结果为1的概率
Drop table if exists long_result1;
create table long_result1 as select row_id,shop_id,
case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability from long_ps_pred10;
--select * from long_result1;

--对概率从大到小进行排序
Drop table if exists l_result;
create table if not exists l_result as
select 
     row_id,shop_id,probability,
     row_number() over(partition by row_id order by probability desc) as prob_rank
from long_result1;
--select * from l_result;

--获取概率最大所对应的shop_id作为预测结果
Drop table if exists long_submission_13;
create table if not exists long_submission_13 as
select 
      row_id,
      concat('s_',shop_id) as shop_id
from l_result
where prob_rank=1;
--select * from long_submission_13;
--select count(distinct row_id) from long_submission_13;

--提交结果
Drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict as
select * from long_submission_13;
--select * from ant_tianchi_ccf_sl_predict;

--保存概率文件
Drop table if exists long_ps_pred_13_20171206;
create table if not exists long_ps_pred_13_20171206 as
select * from long_ps_pred10;
select * from long_ps_pred_13_20171206;

--保存概率为1的文件
Drop table if exists long_ps_pred111_13_20171206;
create table if not exists long_ps_pred111_13_20171206 as
select * from long_result1;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--xgb调参4500
drop table if exists long_xgb_pred_tiaocan4500;
DROP OFFLINEMODEL IF EXISTS long_xgboost_tiaocan4500;

-- train
PAI
-name xgboost
-project algo_public
-Deta="0.08"
-Dobjective="binary:logistic"
-DitemDelimiter=","
-Dseed="0"
-Dnum_round="4500"
-DlabelColName="label"
-DinputTableName="long_ps_train10"
-DenableSparse="false"
-Dmax_depth="8"
-Dsubsample="0.8"
-Dcolsample_bytree="0.8"
-DmodelName="long_xgboost_tiaocan4500"
-Dgamma="0"
-Dlambda="50" 
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,longitudecha,latitudecha,reallongitudecha,reallatitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_transaction_hourcha,shop_user_longitudecha,shop_user_latitudecha,shop_user_wifisignalcha,longlat2,reallonglat2"
-Dbase_score="0.11"
-Dmin_child_weight="100"
-DkvDelimiter=":";

-- predict
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="row_id,shop_id"
-DmodelName="long_xgboost_tiaocan4500"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="long_xgb_pred_tiaocan4500"
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,longitudecha,latitudecha,reallongitudecha,reallatitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_transaction_hourcha,shop_user_longitudecha,shop_user_latitudecha,shop_user_wifisignalcha,longlat2,reallonglat2"
-DinputTableName="long_ps_test10"
-DenableSparse="false";
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--xgb去掉特征shopreallongitude and latitude。
drop table if exists long_xgb_pred_delete1;
DROP OFFLINEMODEL IF EXISTS long_xgboost_delete1;

-- train
PAI
-name xgboost
-project algo_public
-Deta="0.08"
-Dobjective="binary:logistic"
-DitemDelimiter=","
-Dseed="0"
-Dnum_round="4500"
-DlabelColName="label"
-DinputTableName="long_ps_train10"
-DenableSparse="false"
-Dmax_depth="8"
-Dsubsample="0.8"
-Dcolsample_bytree="0.8"
-DmodelName="long_xgboost_delete1"
-Dgamma="0"
-Dlambda="50" 
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,longitudecha,latitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_transaction_hourcha,shop_user_longitudecha,shop_user_latitudecha,shop_user_wifisignalcha,longlat2"
-Dbase_score="0.11"
-Dmin_child_weight="100"
-DkvDelimiter=":";

-- predict
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="row_id,shop_id"
-DmodelName="long_xgboost_delete1"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="long_xgb_pred_delete1"
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_transaction_hour_avg,shop_user_longitude,shop_user_latitude,shop_user_wifisignal_avg,longitudecha,latitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_transaction_hourcha,shop_user_longitudecha,shop_user_latitudecha,shop_user_wifisignalcha,longlat2"
-DinputTableName="long_ps_test10"
-DenableSparse="false";


-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--xgb去掉特征 shop_user_longitude,lat,hour
drop table if exists long_xgb_pred_delete2;
DROP OFFLINEMODEL IF EXISTS long_xgboost_delete2;

-- train
PAI
-name xgboost
-project algo_public
-Deta="0.08"
-Dobjective="binary:logistic"
-DitemDelimiter=","
-Dseed="0"
-Dnum_round="4500"
-DlabelColName="label"
-DinputTableName="long_ps_train10"
-DenableSparse="false"
-Dmax_depth="8"
-Dsubsample="0.8"
-Dcolsample_bytree="0.8"
-DmodelName="long_xgboost_delete2"
-Dgamma="0"
-Dlambda="50" 
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_wifisignal_avg,longitudecha,latitudecha,reallongitudecha,reallatitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_wifisignalcha,longlat2,reallonglat2"
-Dbase_score="0.11"
-Dmin_child_weight="100"
-DkvDelimiter=":";

-- predict
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="row_id,shop_id"
-DmodelName="long_xgboost_delete2"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="long_xgb_pred_delete2"
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="user_id,shop_id,transaction_hour,transaction_weekday,isoweekend,daytime_part,longitude,latitude,is_iphone,signal_avg,flag_avg,wifi10_bssid,wifi1_bssid,wifi2_bssid,wifi3_bssid,wifi4_bssid,wifi5_bssid,wifi6_bssid,wifi7_bssid,wifi8_bssid,wifi9_bssid,wifi1_signal,wifi2_signal,wifi3_signal,wifi4_signal,wifi5_signal,wifi6_signal,wifi7_signal,wifi8_signal,wifi9_signal,wifi10_signal,wifi1_flag,wifi2_flag,wifi3_flag,shop_signal_avg,shop_flag_avg,shop_longitude_avg,shop_latitude_avg,shop_transaction_hour_avg,shop_transaction_lv,shop_signal_avg_20percent,shop_flag_avg_20percent,wifi_number,wifi_in_sw5_num,wifi_in_sw5_lv,wifi_in_sw10_num,wifi_in_sw10_lv,wifi_in_sw20_num,wifi_in_sw20_lv,wifi_in_sw30_num,wifi_in_sw30_lv,wifi_in_sw40_num,wifi_in_sw40_lv,wifi_in_sw50_num,wifi_in_sw50_lv,wifi_in_sw60_num,wifi_in_sw60_lv,wifi_in_sw70_num,wifi_in_sw70_lv,wifi_in_sw80_num,wifi_in_sw80_lv,wifi_in_sw90_num,wifi_in_sw90_lv,wifi_in_sw100_num,wifi_in_sw100_lv,swifi1_signal,swifi1_flag,wifi1_discover_lv,shop_wifi1_longitude,shop_wifi1_latitude,swifi2_signal,swifi2_flag,wifi2_discover_lv,shop_wifi2_longitude,shop_wifi2_latitude,swifi3_signal,swifi3_flag,wifi3_discover_lv,shop_wifi3_longitude,shop_wifi3_latitude,swifi4_signal,swifi4_flag,wifi4_discover_lv,shop_wifi4_longitude,shop_wifi4_latitude,swifi5_signal,swifi5_flag,wifi5_discover_lv,shop_wifi5_longitude,shop_wifi5_latitude,swifi6_signal,swifi6_flag,wifi6_discover_lv,shop_wifi6_longitude,shop_wifi6_latitude,swifi7_signal,swifi7_flag,wifi7_discover_lv,shop_wifi7_longitude,shop_wifi7_latitude,swifi8_signal,swifi8_flag,wifi8_discover_lv,shop_wifi8_longitude,shop_wifi8_latitude,swifi9_signal,swifi9_flag,wifi9_discover_lv,shop_wifi9_longitude,shop_wifi9_latitude,swifi10_signal,swifi10_flag,wifi10_discover_lv,shop_wifi10_longitude,shop_wifi10_latitude,category_id,mall_id,slongitude,slatitude,price,shop_user_transaction_lv,shop_user_wifisignal_avg,longitudecha,latitudecha,reallongitudecha,reallatitudecha,wifi1_signalcha,wifi2_signalcha,wifi3_signalcha,wifi4_signalcha,wifi5_signalcha,wifi6_signalcha,wifi7_signalcha,wifi8_signalcha,wifi9_signalcha,wifi10_signalcha,shop_wifi1_longitudecha,shop_wifi2_longitudecha,shop_wifi3_longitudecha,shop_wifi4_longitudecha,shop_wifi5_longitudecha,shop_wifi6_longitudecha,shop_wifi7_longitudecha,shop_wifi8_longitudecha,shop_wifi9_longitudecha,shop_wifi10_longitudecha,shop_wifi1_latitudecha,shop_wifi2_latitudecha,shop_wifi3_latitudecha,shop_wifi4_latitudecha,shop_wifi5_latitudecha,shop_wifi6_latitudecha,shop_wifi7_latitudecha,shop_wifi8_latitudecha,shop_wifi9_latitudecha,shop_wifi10_latitudecha,lv_5_10,lv_10_20,lv_20_30,lv_30_40,lv_40_50,lv_50_60,lv_60_70,lv_70_80,lv_80_90,lv_90_100,shop_signal_avgcha,shop_flag_avgcha,wifi1_flagcha,wifi2_flagcha,wifi3_flagcha,shop_transaction_hourcha,shop_user_wifisignalcha,longlat2,reallonglat2"
-DinputTableName="long_ps_test10"
-DenableSparse="false";