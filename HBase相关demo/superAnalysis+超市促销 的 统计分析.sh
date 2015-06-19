####################################################################################################                   
# Content: super_detail                           
####################################################################################################



#收藏促销商品：从首页进入
select sum(CPV) from super_detail where dt='2015-04-18'and (funtype='ProFavIndex' and ctid='4275');
11
#收藏促销商品：从列表页进入
select sum(CPV) from super_detail where dt='2015-04-18'and (funtype='ProFavList' and ctid='4275');
1
#取消收藏促销商品：从首页进入
select sum(CPV) from super_detail where dt='2015-04-18'and (funtype='ProNotFavIndex' and ctid='4275');
7
#取消收藏促销商品：从列表页进入
select sum(CPV) from super_detail where dt='2015-04-18'and (funtype='ProNotFavList' and ctid='4275');
1
#点击比价按钮
select sum(CPV) from super_detail where dt='2015-04-18'and (funtype='PriceCompare' and ctid='4275');
1
#点击进入超市门店列表页
select sum(CPV) from super_detail where dt='2015-04-18'and (funtype='StoreList' and ctid='4275');
19
#点击搜索框
select sum(CPV) from super_detail where dt='2015-04-18'and (funtype='SearchBox' and ctid='4275');
7



select * from express_top limit 5;
select count(*) from express_top;



####################################################################################################                           
# Content: express_item                           
####################################################################################################


#查询次数
select sum(pv) from express_item where funtype='searchExpress' and ctid='4275';
985974
#有结果次数
select sum(pv) from express_item where funtype='searchExpress_y' and ctid='4275';
650579
#无结果次数
select sum(pv) from express_item where funtype='searchExpress_n' and ctid='4275';
223262
#无匹配次数
select sum(pv) from express_item where funtype='searchExpress_e' and ctid='4275';
112133

