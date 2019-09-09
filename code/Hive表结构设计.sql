

---------------------------------------------------------------------------------
--                                 插入标签的表
---------------------------------------------------------------------------------
CREATE TABLE `dw.profile_tag_userid`(
`tagid` string COMMENT 'tagid', 
`userid` string COMMENT 'userid', 
`tagweight` string COMMENT 'tagweight', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2', 
`reserve3` string COMMENT '预留3')
COMMENT 'tagid维度userid 用户画像数据'
PARTITIONED BY ( `data_date` string COMMENT '数据日期', `tagtype` string COMMENT '标签主题分类')



CREATE TABLE `dw.profile_tag_cookieid`(
`tagid` string COMMENT 'tagid', 
`cookieid` string COMMENT 'cookieid', 
`tagweight` string COMMENT '标签权重', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2', 
`reserve3` string COMMENT '预留3')
COMMENT 'tagid维度的用户cookie画像数据'
PARTITIONED BY ( `data_date` string COMMENT '数据日期', `tagtype` string COMMENT '标签主题分类')

--------------------------------- 插入几条测试数据-----------------------------------------
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='user_install_days')
values('A220U029_001','25083679','282','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='user_install_days')
values('A220U029_001','7306783','166','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='user_install_days')
values('A220U029_001','4212236','458','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='user_install_days')
values('A220U029_001','39730187','22','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='user_install_days')
values('A220U029_001','16254215','57','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='userid_all_paid_money')
values('A220U083_001','25083679','800.39','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='userid_all_paid_money')
values('A220U083_001','7306783','311.29','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='userid_all_paid_money')
values('A220U083_001','32171777','129.65','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='userid_all_paid_money')
values('A220U083_001','40382657','602.3','','',''); 
insert into table dw.profile_tag_userid  partition(data_date='20180421', tagtype='userid_all_paid_money')
values('A220U083_001','30765587','465.93','','',''); 
--------------------------------------------------------------------------------------------



---------------------------------------------------------------------------------
--                                标签转换的表
---------------------------------------------------------------------------------
CREATE TABLE `dw.profile_user_map_cookieid`(
`cookieid` string COMMENT 'tagid', 
`tagsmap` map<string,string> COMMENT 'cookieid', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2')
COMMENT 'cookie 用户画像数据'
PARTITIONED BY (`data_date` string COMMENT '数据日期')



CREATE TABLE `dw.profile_user_map_userid`(
`userid` string COMMENT 'userid', 
`tagsmap` map<string,string> COMMENT 'tagsmap', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2')
COMMENT 'userid 用户画像数据'
PARTITIONED BY (`data_date` string COMMENT '数据日期')



--标签聚合
	insert overwrite table dw.profile_user_map_userid  partition(data_date="20180421") 
	select userid,
		   str_to_map(concat_ws(',',collect_set(concat(tagid,':',tagweight)))) as tagsmap,
		   '',
		   '' 
	  from dw.profile_tag_userid 
	 where data_date="20180421" 
  group by userid


--------------------------------------------------------------------------------------------



--------------------------------------------------------
--                        人群 
--------------------------------------------------------

CREATE TABLE `dw.profile_user_group`(
`id` string, 
`tagsmap` map<string,string>, 
`reserve` string, 
`reserve1` string)
PARTITIONED BY (`data_date` string, `target` string)





--------------------------------------------------------
--                    hive 向hbase映射数据
--------------------------------------------------------

hive --auxpath $HIVE_HOME/lib/zookeeper-3.4.6.jar,$HIVE_HOME/lib/hive-hbase-handler-2.3.3.jar,$HIVE_HOME/lib/hbase-server-1.1.1.jar --hiveconf hbase.zookeeper.quorum=master,node-1,node-2


CREATE TABLE dw.userprofile_hive_2_hbase
(key string, 
value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val")
TBLPROPERTIES ("hbase.table.name" = "userprofile_hbase");

INSERT OVERWRITE TABLE dw.userprofile_hive_2_hbase 
SELECT userid,tagid FROM dw.profile_tag_userid;



