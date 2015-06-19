####################################################################################################                 
# Content: express_top (Partition)（校验数据MR的结果）                    
####################################################################################################

# DROP TABLE IF EXISTS express_top;
# CREATE EXTERNAL TABLE IF NOT EXISTS express_top(
# table_name string,
# datatime string,
# os string,
# ver string,
# ctid string,
# funtype string,
# objectid string,
# uv bigint,
# pv bigint,
# pv_y bigint,
# pv_n bigint,
# pv_e bigint
# )
# ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
# STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
# # add data	according to partition, so it will err
# ALTER TABLE express_top SET LOCATION 'hdfs:/test/sibyl/transform/2015-04-14/express/DA_ExpressTop_Analysis';

# 第一步：建表，建分区
DROP TABLE IF EXISTS express_top;
CREATE EXTERNAL TABLE IF NOT EXISTS express_top(
table_name string,
datatime string,
os string,
ver string,
ctid string,
funtype string,
objectid string,
uv bigint,
pv bigint,
pv_y bigint,
pv_n bigint,
pv_e bigint
)
PARTITIONED BY (dt string)
##
#‘\t’这段的意思是字段与字段之间用\t分割，所以load数据之前要看看你文件的分割符是不是\t，如果不是就会出错，后面很多null等着你哦。
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
# 第二步：Load数据，指定分区
ALTER TABLE express_top ADD PARTITION(dt='2015-04-14') LOCATION 'hdfs:/test/sibyl/transform/express/DA_ExpressItem_Analysis/2015-04-14';
ALTER TABLE express_top ADD PARTITION(dt='2015-04-15') LOCATION 'hdfs:/test/sibyl/transform/express/DA_ExpressItem_Analysis/2015-04-15';


# 凡是有分区的查询都要指定分区
select * from express_top where dt='2015-04-14' limit 5;
select count(*) from express_top;



####################################################################################################                       
# Content: express_item（校验数据MR的结果）                      
####################################################################################################

# 第一步：建表和指定路径，一次插入，多次查询
DROP TABLE IF EXISTS express_item;
CREATE EXTERNAL TABLE IF NOT EXISTS express_item(
table_name string,
datatime string,
os string,
ver string,
ctid string,
funtype string,
uv bigint,
pv bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/test/sibyl/transform/express/DA_ExpressItem_Analysis/2015-04-15';


select * from express_item limit 5;
select count(*) from express_item;

####################################################################################################                          
# Content: express_item (Partition)（数据恢复——快递分析）                        
####################################################################################################

# 第一步：建表，建分区
DROP TABLE IF EXISTS express_item;
CREATE EXTERNAL TABLE IF NOT EXISTS express_item(
table_name string,
datatime string,
os string,
ver string,
ctid string,
funtype string,
uv bigint,
pv bigint
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
# 第二步：Load数据，指定分区
ALTER TABLE express_item ADD PARTITION(dt='2015-04-14') LOCATION 'hdfs:/test/sibyl/transform/express/DA_ExpressItem_Analysis/2015-04-14';
ALTER TABLE express_item ADD PARTITION(dt='2015-04-15') LOCATION 'hdfs:/test/sibyl/transform/express/DA_ExpressItem_Analysis/2015-04-15';
ALTER TABLE express_item DROP PARTITION(dt='2015-04-15');
ALTER TABLE express_item ADD PARTITION(dt='2015-04-16') LOCATION 'hdfs:/test/sibyl/transform/express/DA_ExpressItem_Analysis/2015-04-16';


# 凡是有分区的查询都要指定分区
select * from express_item where dt='2015-04-14' limit 5;
select * from express_item where dt='2015-04-15' limit 5;
select * from express_item where dt='2015-04-16' limit 5;
select count(*) from express_item;



####################################################################################################                    
# Content: super_detail（校验数据MR的结果）                           
####################################################################################################

# 第一步：建表，建分区
DROP TABLE IF EXISTS super_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS super_detail(
table_name string,
datatime string,
os string,
ctid string,
funtype string,
CPV bigint,
UV bigint
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

# 第二步：Load数据，指定分区
ALTER TABLE super_detail ADD PARTITION(dt='2015-04-18') LOCATION 'hdfs:/test/sibyl/transform/superSeckill/DA_SuperDetail_Analysis/2015-04-18';

select * from super_detail where dt='2015-04-18' limit 5;


select count(*) from super_detail;

####################################################################################################                           
# Content: scan_analysis（校验数据MR的结果）                           
####################################################################################################

# 第一步：建表，建分区
DROP TABLE IF EXISTS scan_analysis;
CREATE EXTERNAL TABLE IF NOT EXISTS scan_analysis(
table_name string,
datatime string,
os string,
ver string,
channel string,
scantype string,
uv bigint,
pv bigint
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

# 第二步：Load数据，指定分区
ALTER TABLE scan_analysis ADD PARTITION(dt='2015-04-18') LOCATION 'hdfs:/test/sibyl/transform/scan/DA_Scan_Analysis/2015-04-18';
ALTER TABLE scan_analysis ADD PARTITION(dt='2015-04-19') LOCATION 'hdfs:/test/sibyl/transform/scan/DA_Scan_Analysis/2015-04-19';

select * from scan_analysis where dt='2015-04-18' limit 5;
select * from scan_analysis where dt='2015-04-19' limit 5;

select count(*) from scan_analysis;


####################################################################################################                          
# Content: scan_analysis（校验汉信码的数据）                           
####################################################################################################

# 第一步：建表和指定路径，一次插入，多次查询
DROP TABLE IF EXISTS scan_analysis;
CREATE EXTERNAL TABLE IF NOT EXISTS scan_analysis(
table_name string,
datatime string,
os string,
ver string,
channel string,
scantype string,
uv bigint,
pv bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/test/sibyl/transform/scan/2015-04-17/DA_Scan_Analysis/';

select * from scan_analysis limit 5;
select * from scan_analysis where scantype='HCode' limit 5;


####################################################################################################                          
# Content: exposure_analysis（校验红榜搜索和黑榜搜索）                           
####################################################################################################

# 第一步：建表和指定路径，一次插入，多次查询
DROP TABLE IF EXISTS exposure_analysis;
CREATE EXTERNAL TABLE IF NOT EXISTS exposure_analysis(
table_name string,
datatime string,
os string,
ver string,
ctid string,
expotype string,
uv bigint,
pv bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/test/sibyl/transform/2015-04-30/DA_ExpoItem_Analysis';

select * from exposure_analysis limit 5;
select * from exposure_analysis where expotype='RedSearch' limit 5;
select sum(pv) from exposure_analysis where expotype='RedSearch' and ctid='4275';

####################################################################################################                            
# Content: point_analysis（校验积分兑换的进入方式）                           
####################################################################################################

# 第一步：建表和指定路径，一次插入，多次查询
DROP TABLE IF EXISTS point_analysis;
CREATE EXTERNAL TABLE IF NOT EXISTS point_analysis(
table_name string,
datatime string,
funtype string,
pv bigint,
uv bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs:/test/sibyl/transform/point/DA_PointItem_Analysis';

select * from point_analysis limit 5;


####################################################################################################                         
# Content: category_analysis（分类数据恢复）                           
####################################################################################################

# 第一步：建表和指定路径，一次插入，多次查询
DROP TABLE IF EXISTS category_analysis;
CREATE EXTERNAL TABLE IF NOT EXISTS category_analysis(
datatime string,
ctid string,
CLType string,
CLName string,
ScanUV bigint,
ScanPV bigint,
BarcodeNum bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs:/test/sibyl/transform/priceCompare/2015-05-14';

select * from category_analysis limit 5;

