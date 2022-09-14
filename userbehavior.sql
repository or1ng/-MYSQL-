-- 数据准备
create database taobao;

use taobao;

create table UserBehavior(
	user_id int,
    item_id int,
	item_category int,
    behavior_type varchar(10),
    user_geohash varchar(10),
    times datetime,
    amount decimal(5,2)
);

show variables like '%secure%';#查看安全路径
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/UserBehavior.csv"
into table UserBehavior
fields terminated by ','
ignore 1 lines;

select * from UserBehavior limit 10;


-- 数据清洗
-- 缺失值处理
select 
	count(user_id),
    count(item_id),
    count(item_category),
    count(behavior_type),
    count(user_geohash),
    count(times),
    count(amount)
from UserBehavior;

-- 异常值检查
select min(times),max(times),min(amount),max(amount) from UserBehavior;


-- 重复记录处理
select distinct * from UserBehavior;


-- 字段处理：根据times字段增加计算字段用户行为日期、周和小时，排除后续分析不需要的user_geohash字段，并将筛选后的结果保存到新表
select 
	user_id,
    item_id,
    item_category,
    behavior_type,
    date(times) as 日期,
    hour(times) as 小时,
    date_format(times,'%w') as 星期,
    amount
from (select distinct * from UserBehavior) as t;

-- 处理结果保存到视图
create view UserBehavior_view as
select 
	user_id,
    item_id,
    item_category,
    behavior_type,
    date(times) as 日期,
    hour(times) as 小时,
    date_format(times,'%w') as 星期,
    amount 
from (select distinct * from UserBehavior) as t;

select * from UserBehavior_view limit 100;
select count(*) from UserBehavior_view;#962097

-- 数据预览
select 
	count(distinct user_id) as 用户数,
    count(distinct item_category) as 类目数,
    count(distinct item_id) as 商品数
from UserBehavior_view;

select behavior_type,count(*) as 行为次数
from UserBehavior_view
group by behavior_type;

-- 数据分析
-- 1.流量指标分析
-- 每日PV、UV、人均浏览量、成交量、销售额
select 
	日期,
    sum(behavior_type='pv') as 浏览量,
    count(distinct user_id) as 访客数,
    sum(behavior_type='pv')/count(distinct user_id) as 人均浏览量,
    sum(behavior_type='buy') as 成交量,
    sum(if(behavior_type='buy',amount,0)) as 销售额
from UserBehavior_view
group by 日期;

-- 周一至周日PV、UV、人均浏览量、成交量、销售额
select 
	星期,
    sum(behavior_type='pv') as 浏览量,
    count(distinct user_id) as 访客数,
    sum(behavior_type='pv')/count(distinct user_id) as 人均浏览量,
    sum(behavior_type='buy') as 成交量,
    sum(if(behavior_type='buy',amount,0)) as 销售额
from UserBehavior_view
group by 星期;

-- 每小时PV、UV、人均浏览量、成交量、销售额
select 
	小时,
    sum(behavior_type='pv') as 浏览量,
    count(distinct user_id) as 访客数,
    sum(behavior_type='pv')/count(distinct user_id) as 人均浏览量,
    sum(behavior_type='buy') as 成交量,
    sum(if(behavior_type='buy',amount,0)) as 销售额
from UserBehavior_view
group by 小时;

-- 2.行为转化分析
select 
	behavior_type,
    count(distinct user_id) as 用户数,
    lag(count(distinct user_id),1) over(order by if(behavior_type='pv',1,if(behavior_type='fav',2,if(behavior_type='cart',3,4)))) as 上一行为用户数,
    count(distinct user_id)/lag(count(distinct user_id),1) over(order by if(behavior_type='pv',1,if(behavior_type='fav',2,if(behavior_type='cart',3,4)))) as 转化率
from userbehavior_view
group by behavior_type;

-- 浏览—加购—购买的转化率
select 
	behavior_type,
    count(distinct user_id) as 用户数,
    lag(count(distinct user_id),1) over(order by if(behavior_type='pv',1,if(behavior_type='cart',2,3))) as 上一行为用户数,
    ifnull(count(distinct user_id)/lag(count(distinct user_id),1) over(order by if(behavior_type='pv',1,if(behavior_type='cart',2,3))),1) as 转化率
from userbehavior_view
where behavior_type in ('pv','cart','buy')
group by behavior_type;

-- 每日浏览—加购—购买的转化率
select 
	日期,
    sum(if(behavior_type='pv',用户数,0)) as 浏览人数,
    sum(if(behavior_type='cart',用户数,0)) as 加购人数,
    sum(if(behavior_type='buy',用户数,0)) as 购买人数,
    sum(if(behavior_type='cart',用户数,0))/sum(if(behavior_type='pv',用户数,0)) as 浏览_加购转化率,
    sum(if(behavior_type='buy',用户数,0))/sum(if(behavior_type='cart',用户数,0)) as 加购_购买转化率
from 
	(select 日期,behavior_type,count(distinct user_id) as 用户数
	from userbehavior_view
	where behavior_type in ('pv','cart','buy')
	group by 日期,behavior_type) as t
group by 日期;

-- 3.产品贡献定量分析（帕累托分析）
select *
from 
	(select 
		item_category,
		sum(amount) as 销售额,
		sum(sum(amount)) over(order by sum(amount) desc) as 累积销售额,
		#当over中指定了排序，但是没有指定滑动窗口范围时，默认计算当前分区内第一行到当前行排序字段取值范围内的记录
		sum(sum(amount)) over() as 总销售额,
		#当over中没有指定排序和滑动窗口范围时，默认计算当前分区内的所有记录
		sum(sum(amount)) over(order by sum(amount) desc)/sum(sum(amount)) over() as 累积销售额百分比
	from userbehavior_view
	where behavior_type='buy'
	group by item_category) as t
where 累积销售额百分比<=0.8;


select 
	item_category,
    sum(amount) as 销售额,
    sum(销售额) over(order by 销售额 desc) as 累积销售额,
    #当over中指定了排序，但是没有指定滑动窗口范围时，默认计算当前分区内第一行到当前行排序字段取值范围内的记录
    sum(销售额) over() as 总销售额
    #当over中没有指定排序和滑动窗口范围时，默认计算当前分区内的所有记录
from userbehavior_view
where behavior_type='buy'
group by item_category;

-- 4.用户价值分析
-- 每个用户消费时间间隔、消费频次、消费金额
select 
	user_id,
    max(日期) as 最近一次消费时间,
    timestampdiff(day,max(日期),'2014-12-19') as 间隔天数,
    count(*) as 消费频率,
    sum(amount) as 消费金额
from userbehavior_view
where behavior_type='buy'
group by user_id;

-- RFM评分
select 
	user_id,
    timestampdiff(day,max(日期),'2014-12-19') as R,
    count(*) as F,
    sum(amount) as M,
    case when timestampdiff(day,max(日期),'2014-12-19')<=6 then 5
		 when timestampdiff(day,max(日期),'2014-12-19')<=12 then 4
         when timestampdiff(day,max(日期),'2014-12-19')<=18 then 3
         when timestampdiff(day,max(日期),'2014-12-19')<=24 then 2
         else 1
	end as R评分,
    if(count(*)=1,1,if(count(*)=2,2,if(count(*)=3,3,if(count(*)=4,4,5)))) as F评分,
    if(sum(amount)<100,1,if(sum(amount)<200,2,if(sum(amount)<300,3,if(sum(amount)<400,4,5)))) as M评分
from userbehavior_view
where behavior_type='buy'
group by user_id;

-- RFM均值
select 
	avg(R评分) as R均值,
    avg(F评分) as F均值,
    avg(m评分) as M均值
from 
	(select 
		user_id,
		case when timestampdiff(day,max(日期),'2014-12-19')<=6 then 5
			 when timestampdiff(day,max(日期),'2014-12-19')<=12 then 4
			 when timestampdiff(day,max(日期),'2014-12-19')<=18 then 3
			 when timestampdiff(day,max(日期),'2014-12-19')<=24 then 2
			 else 1
		end as R评分,
		if(count(*)=1,1,if(count(*)=2,2,if(count(*)=3,3,if(count(*)=4,4,5)))) as F评分,
		if(sum(amount)<100,1,if(sum(amount)<200,2,if(sum(amount)<300,3,if(sum(amount)<400,4,5)))) as M评分
	from userbehavior_view
	where behavior_type='buy'
	group by user_id) as t;

-- RFM重要程度
select 
	*,
    if(R评分>3.5984,'高','低') as R程度,
    if(F评分>2.1039,'高','低') as F程度,
    if(M评分>2.2051,'高','低') as M程度
from 
	(select 
		user_id,
		timestampdiff(day,max(日期),'2014-12-19') as R,
		count(*) as F,
		sum(amount) as M,
		case when timestampdiff(day,max(日期),'2014-12-19')<=6 then 5
			 when timestampdiff(day,max(日期),'2014-12-19')<=12 then 4
			 when timestampdiff(day,max(日期),'2014-12-19')<=18 then 3
			 when timestampdiff(day,max(日期),'2014-12-19')<=24 then 2
			 else 1
		end as R评分,
		if(count(*)=1,1,if(count(*)=2,2,if(count(*)=3,3,if(count(*)=4,4,5)))) as F评分,
		if(sum(amount)<100,1,if(sum(amount)<200,2,if(sum(amount)<300,3,if(sum(amount)<400,4,5)))) as M评分
	from userbehavior_view
	where behavior_type='buy'
	group by user_id) as t;

-- RFM用户价值
select 
	*,
    case when R程度='高' and F程度='高' and M程度='高' then '重要价值用户'
		 when R程度='高' and F程度='低' and M程度='高' then '重要发展用户'
         when R程度='低' and F程度='高' and M程度='高' then '重要保持用户'
         when R程度='低' and F程度='低' and M程度='高' then '重要挽留用户'
         when R程度='高' and F程度='高' and M程度='低' then '一般价值用户'
         when R程度='高' and F程度='低' and M程度='低' then '一般发展用户'
         when R程度='低' and F程度='高' and M程度='低' then '一般保持用户'
         else '一般挽留用户'
	end as 用户价值分类
from 
	(select 
		*,
		if(R评分>3.5984,'高','低') as R程度,
		if(F评分>2.1039,'高','低') as F程度,
		if(M评分>2.2051,'高','低') as M程度
	from 
		(select 
			user_id,
			timestampdiff(day,max(日期),'2014-12-19') as R,
			count(*) as F,
			sum(amount) as M,
			case when timestampdiff(day,max(日期),'2014-12-19')<=6 then 5
				 when timestampdiff(day,max(日期),'2014-12-19')<=12 then 4
				 when timestampdiff(day,max(日期),'2014-12-19')<=18 then 3
				 when timestampdiff(day,max(日期),'2014-12-19')<=24 then 2
				 else 1
			end as R评分,
			if(count(*)=1,1,if(count(*)=2,2,if(count(*)=3,3,if(count(*)=4,4,5)))) as F评分,
			if(sum(amount)<100,1,if(sum(amount)<200,2,if(sum(amount)<300,3,if(sum(amount)<400,4,5)))) as M评分
		from userbehavior_view
		where behavior_type='buy'
		group by user_id) as t1) as t2;








