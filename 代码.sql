drop database share_bike;
create database share_bike location "/share_bike";
use share_bike;

-- 自定义函数
-- 将数据转化为json格式
create  function tmpsql.to_json as "MyUDF.UDF.ToJson"
    using jar "hdfs://192.168.40.11:8020/hiveJars/allFunction.jar" ;
-- 将经纬度转化为geohash
create function tmpsql.geohash as "MyUDF.UDF.GetGeoHash"
    using jar "hdfs://192.168.40.11:8020/hiveJars/allFunction.jar";
-- 根据baidu地图接口得到地区
create function tmpsql.location_baidu as "MyUDF.UDF.LocationBaiduAPI"
    using jar "hdfs://192.168.40.11:8020/hiveJars/allFunction.jar";
-- 得到历史天气
create function tmpsql.history_weather as "MyUDF.UDF.Weather"
    using jar "hdfs://192.168.40.11:8020/hiveJars/allFunction.jar";
-- 计算两个经纬度之间的距离
create function tmpsql.distance as "MyUDF.UDF.Distance"
    using jar "hdfs://192.168.40.11:8020/hiveJars/allFunction.jar";
-- 根据经纬度离线得到区县信息
create function tmpsql.location as "MyUDF.UDF.GetLocationS2"
    using jar "hdfs://192.168.40.11:8020/hiveJars/allFunction.jar";


-- ods层
-- ods层采用gzip压缩(hadoop自带) gzip压缩比例高,解压比较慢, ods层不常读取数据,所以选择它
-- TextFile文件格式无法直接指定压缩格式，但可以通过配置参数进行控制
-- SET hive.exec.compress.output=true; 开启输出结果压缩功能
-- 压缩要跑task,load data 本质是hadoop fs -put

SET hive.exec.compress.output=true;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table ods_ride_log;
create  table ods_ride_log(
                              orderid string comment "订单id",
                              userid string comment "用户id",
                              bikeid string comment "自行车id",
                              start_time string comment "骑行开始时间",
                              start_lng string comment "骑行开始经度",
                              start_lat string comment "骑行开始纬度",
                              end_time string comment "骑行结束时间",
                              end_lng string comment "骑行结束经度",
                              end_lat string comment "骑行结束纬度",
                              lock_status string comment "锁的状态"
)comment "用户骑行日志"
partitioned by (dt string)
row format delimited fields terminated by ","
stored as Textfile
tblproperties ("compress"="gzip");

drop table ods_ride_load_data_tmp;
create table ods_ride_load_data_tmp(
                                       orderid string comment "订单id",
                                       userid string comment "用户id",
                                       bikeid string comment "自行车id",
                                       start_time string comment "骑行开始时间",
                                       start_lng string comment "骑行开始经度",
                                       start_lat string comment "骑行开始纬度",
                                       end_time string comment "骑行结束时间",
                                       end_lng string comment "骑行结束经度",
                                       end_lat string comment "骑行结束纬度",
                                       lock_status string comment "锁的状态"
)comment "用于装载用户骑行日志的临时表"
row format delimited fields terminated by ",";

load data inpath "/data/ride.log" overwrite into table ods_ride_load_data_tmp;

insert overwrite table ods_ride_log
select
    *,
    "2024-01-01" -- 分区字段
from ods_ride_load_data_tmp;

drop table ods_bike_info;
create  table ods_bike_info(
                               bikeid string comment "自行车id",
                               bike_type int comment "自行车类型",
                               bike_start_time string comment "自行车投入时间",
                               bike_status string comment "自行车状态"
)comment "自行车信息"
partitioned by (month_cnt string)
row format delimited fields terminated by ","
stored as Textfile
tblproperties ("compress"="gzip");

drop table ods_bike_load_data_tmp;
create table ods_bike_load_data_tmp(
                                       bikeid string comment "自行车id",
                                       bike_type int comment "自行车类型",
                                       bike_start_time string comment "自行车投入时间",
                                       bike_status string comment "自行车状态"
)comment "用于装载单车信息的临时表"
row format delimited fields terminated by ",";

load data inpath "/data/bike.csv" overwrite into table ods_bike_load_data_tmp;

set mapred.reduce.tasks  = 2; --避免产生太多空文件
insert overwrite table ods_bike_info
select
    *,
    month("2024-01-01")  --以月分区
from ods_bike_load_data_tmp;

-- 增量数据 监听binlog日志
drop table ods_user_info;
create table ods_user_info(
                              userid string comment "id",
                              name string comment "姓名",
                              gender string comment "性别",
                              type string comment "用户类型",
                              identity_card string comment "身份证号",
                              salary string comment "薪资",
                              phone_num string comment "手机号",
                              job string comment "职业",
                              education string comment "学历",
                              register_dt string comment "注册时间",
                              email string comment "邮箱",
                              register_location string comment "注册地",
                              user_level int comment "用户等级"
)comment "用户信息"
partitioned by (week_cnt string)
row format delimited fields terminated by ","
stored as Textfile
tblproperties ("compress"="gzip");


drop table ods_user_load_data_tmp;
create table ods_user_load_data_tmp (
                                        userid string comment "id",
                                        name string comment "姓名",
                                        gender string comment "性别",
                                        type string comment "用户类型",
                                        identity_card string comment "身份证号",
                                        salary string comment "薪资",
                                        phone_num string comment "手机号",
                                        job string comment "职业",
                                        education string comment "学历",
                                        register_dt string comment "注册时间",
                                        email string comment "邮箱",
                                        register_location string comment "注册地",
                                        user_level int comment "用户等级"
)comment "用于装载用户信息的临时表"
row format delimited fields terminated by ",";

load data inpath "/data/userinfo.csv" overwrite into table ods_user_load_data_tmp;

insert overwrite table ods_user_info
select
    *,
    weekofyear("2024-01-01")
from ods_user_load_data_tmp;

-- dim
-- 天气,地区编码,日期,geohash表,单车表
-- 5位长度的geohash误差为方圆2.5KM

set mapred.reduce.tasks = 1; --避免产生太多小文件,只生成一个文件

--该表数据几乎不变,不用每日调度
drop table dim_city_level;
create table dim_city_level(
                               city_code string comment "城市编码",
                               name string comment "城市名",
                               levle string comment "等级"
)row format delimited fields terminated by ",";

load data inpath "/data/cityLevel.csv" overwrite into table  dim_city_level;

--该表数据几乎不变,不用每日调度
drop table dim_city_code_nine_version;
create table dim_city_code_nine_version(
                                           code int comment "地区编码",
                                           name string comment "地区名"
)comment "地区编码九位用于查询当前天气"
row format delimited fields terminated by "=";

load data inpath "/data/cityCode.txt" overwrite into table  dim_city_code_nine_version;

--该表数据几乎不变,不用每日调度
drop table dim_city_code_special_version;
create table dim_city_code_special_version(
                                              code int comment "地区编码",
                                              name string comment "地区名"
)comment "地区编码五位用于查询历史天气"
row format delimited fields terminated by "=";

load data inpath "/data/cityCode1.txt" overwrite into table  dim_city_code_special_version;

-- 该表数据一年才改变一次,不用每日调度
drop table dim_date;
create table dim_date(
                         `year` int ,
                         `date` string,
                         `week` string,
                         `week_cnt` int,
                         status string comment "上班 周末 节假日",
                         status_code int comment "0 上班 1周末 2节假日",
                         is_workday string,
                         is_workday_code int comment "0 是 1 否"
)comment"日期表"
row format delimited fields terminated by ",";

load data inpath "/data/dt.txt" into table dim_date;

drop table dim_user_info_zip;
create external table dim_user_info_zip(
            userid string comment "id",
            name string comment "姓名",
            gender string comment "性别",
            type string comment "用户类型",
            identity_card string comment "身份证号",
            salary string comment "薪资",
            phone_num string comment "手机号",
            job string comment "职业",
            education string comment "学历",
            register_dt string comment "注册时间",
            email string comment "邮箱",
            register_location string comment "注册地",
            user_level string comment "用户等级",
            start_time string,
            end_time string
)comment "用户信息拉链表"
partitioned by (dt string)
row format delimited fields terminated by ","
stored as orc
tblproperties ('compress' = 'snappy');

-- 首日装载数据
insert overwrite table dim_user_info_zip
select
    userid,
    concat(substr(name,1,1),replace(space(length(name)-1)," ","*")),
    gender ,
    type     ,
    identity_card ,
    salary  ,
    md5(phone_num)  ,
    job   ,
    education  ,
    register_dt    ,
    md5(email)    ,
    register_location ,
    user_level,
    date_sub(`current_date`(),1) as start_time,
    "9999-12-31",
    "9999-12-31"
from ods_user_info;

-- 以后装载数据
with a as (
    select
        *
    from dim_user_info_zip
    union
    select
        *,
        date_sub(`current_date`(),1) as start_time,
        "9999-12-31"
    from ods_user_info
),
b as (
    select
        *,
        -- 如果rk >= 2 则说明用户修改了信息
        rank() over (partition by userid order by start_time desc ) as rk
    from a
)
insert overwrite table dim_user_info_zip
select
    userid ,
    name ,
    gender ,
    type ,
    identity_card ,
    salary ,
    phone_num ,
    job ,
    education ,
    register_dt ,
    email ,
    register_location ,
    user_level ,
    start_time,
    `if`(rk == 2,date_sub(`current_date`(),1),end_time),
    `if`(rk == 2,date_sub(`current_date`(),1),end_time)
from b;


drop table dim_bike_info;
create external table dim_bike_info(
    bikeid string,
    bike_type string,
    bike_type_code int comment "0 电动车 1 普通自行车 2 固定式自行车(带桩停车)",
    bike_start_time string,
    bike_status string,
    bike_status_code int comment "0 正常 1 异常"
)comment "单车信息维表"
partitioned by (month_cnt int)
row format delimited fields terminated by ","
stored as orc
tblproperties ("compress"="snappy");

insert overwrite table dim_bike_info
select
    bikeid,
    case when replace(bike_type,'"','') = "0" then "电动自行车"
         when replace(bike_type,'"','') = "1" then "传统自行车"
         else "固定式自行车(带桩停车)"
        end,
    bike_type,
    bike_start_time,
    `if`(bike_status=0,"正常","异常"),
    bike_status,
    month("2024-01-01")
from ods_bike_info;

drop table dim_geo_hash;
create table dim_geo_hash(
                             district_code string,
                             province string,
                             city string,
                             district string,
                             lng decimal(10,6),
                             lat decimal(10,6),
                             geo_hash string
)comment "geohash表"
row format delimited fields terminated by ",";

load data inpath "/data/geohash.csv" into table dim_geo_hash;

drop table dim_weather;
create table dim_weather(
                            dt string comment "日期",
                            city_code int comment "地区编码",
                            city string comment "地区名称",
                            weather INT COMMENT '天气情况',
                            temp FLOAT COMMENT '温度',
                            hum FLOAT COMMENT '湿度',
                            windspeed FLOAT COMMENT '风速'
)comment "存储天气相关信息"
partitioned by (year_cnt int )
stored as orc
tblproperties ("compress"="snappy");


with a as (
    select
        date_format(start_time,"yyyy-MM-dd") as dt,
        geohash(start_lng,start_lat,5) as geo,
        start_lng,
        start_lat
    from ods_ride_log
),
     b as (
         select
             dt,
             code,
             district
         from (
                  select
                      a.dt as dt,
                      coalesce(dh.district_code,
                               get_json_object(location(start_lng,start_lat),"$.districtCode"),
                               get_json_object(location_baidu(cast(start_lng as double),cast(start_lat as double)),"$.adcode")
                          ) as code,
                      coalesce(dh.district,
                               get_json_object(location(start_lng,start_lat),"$.district"),
                               get_json_object(location_baidu(cast(start_lng as double),cast(start_lat as double)),"$.district")
                          ) as district
                  from a
                           join dim_geo_hash dh
                                on a.geo = dh.geo_hash
                  )t group by dt, code,district
     ),
     c as (
         select
             b.*,
             history_weather(dv.code,b.dt) as weather
         from b
                  join  dim_city_code_special_version dv
                        on b.district like concat(dv.name,"%")
     )
insert overwrite table dim_weather
select
    dt,
    code,
    district,
    get_json_object(weather,"$.weather"),
    get_json_object(weather,"$.temp"),
    get_json_object(weather,"$.hum"),
    get_json_object(weather,"$.windSpeed"),
    year(`current_date`())
from c;


-- cdm 公共维度模型层

drop table cdm_user_info_topic;
create external table cdm_user_info_topic(
        userid  string comment "id",
        gender  string comment "性别",
        age int comment "年龄",
        age_range string comment "年龄阶段",
        usertype              string comment "用户类型",
        salary            string comment "收入",
        job               string comment "职业",
        education   string comment "学历",
        register_dt       string comment "注册时间",
        register_year int comment "注册时间年",
        register_month int comment "注册时间月",
        register_location string comment "注册地",
        user_levle int comment "用户等级"
)comment "用户信息主题"
partitioned by (dt string)
row format delimited fields terminated by ","
stored as orc
tblproperties ('compress' = 'snappy');


insert overwrite table cdm_user_info_topic
select
    dz.userid,
    case when gender = 1 then "男"
         when  gender = 2 then "女"
         else  "未知" end ,
    year(`current_date`()) - substr(identity_card,7,4),
    case when year(`current_date`()) - substr(identity_card,7,4) <=20 then "20岁以下"
         when year(`current_date`()) - substr(identity_card,7,4) >20 and year(`current_date`()) - substr(identity_card,7,4) <=30 then "20-30岁"
         when year(`current_date`()) - substr(identity_card,7,4) >30 and year(`current_date`()) - substr(identity_card,7,4) <=40 then "30-40岁"
         when year(`current_date`()) - substr(identity_card,7,4) >40 and year(`current_date`()) - substr(identity_card,7,4) <=50 then "40-50岁"
         when year(`current_date`()) - substr(identity_card,7,4) >50 and year(`current_date`()) - substr(identity_card,7,4) <=60 then "50-60岁"
         else "60岁以上" end ,
    type,
    salary,
    job,
    education,
    register_dt,
    year(register_dt),
    month(register_dt),
    register_location,
    dz.user_level,
    "2024-01-01"
from dim_user_info_zip dz
where dt = "9999-12-31";


drop table cdm_user_ride_topic;
create table cdm_user_ride_topic(
                                    orderid string comment "订单id",
                                    userid string comment "用户id",
                                    age_range string comment "年龄范围",
                                    gender string comment "性别",
                                    bikeid string comment "自行车id",
                                    bike_type string comment "自行车类型",
                                    start_time string comment "骑行开始时间",
                                    start_lng string comment "骑行开始经度",
                                    start_lat string comment "骑行开始纬度",
                                    end_time string comment "骑行结束时间",
                                    end_lng string comment "骑行结束经度",
                                    end_lat string comment "骑行结束纬度",
                                    trip_duration int comment "骑行时间",
                                    distance string comment "骑行距离"
)comment "用户骑行主题"
partitioned by (dt string)
row format delimited fields terminated by ","
stored as orc
tblproperties ("compress"="snappy");


insert overwrite table cdm_user_ride_topic
select
    ol.orderid ,
    ol.userid ,
    case when year(`current_date`()) - substr(identity_card,7,4) <=20 then "20岁以下"
         when year(`current_date`()) - substr(identity_card,7,4) >20 and year(`current_date`()) - substr(identity_card,7,4) <=30 then "20-30岁"
         when year(`current_date`()) - substr(identity_card,7,4) >30 and year(`current_date`()) - substr(identity_card,7,4) <=40 then "30-40岁"
         when year(`current_date`()) - substr(identity_card,7,4) >40 and year(`current_date`()) - substr(identity_card,7,4) <=50 then "40-50岁"
         when year(`current_date`()) - substr(identity_card,7,4) >50 and year(`current_date`()) - substr(identity_card,7,4) <=60 then "50-60岁"
         else "60岁以上" end  ,
    case when gender = 1 then "男"
         when  gender = 2 then "女"
         else  "未知" end   as gender,
    ol.bikeid ,
    bike_type ,
    ol.start_time ,
    ol.start_lng ,
    ol.start_lat ,
    ol.end_time ,
    ol.end_lng  ,
    ol.end_lat  ,
    to_unix_timestamp(ol.end_time) - to_unix_timestamp(ol.start_time) ,
    distance(start_lng,start_lat,end_lng,end_lat),
    "2024-01-01"
from ods_ride_log ol
         left join dim_bike_info bike
                   on ol.bikeid = bike.bikeid
         left join dim_user_info_zip dz
                   on ol.userid = dz.userid;

drop table cdm_user_time_topic;
create table cdm_user_time_topic(
                                    dt string comment "日期",
                                    `time` string comment "时刻",
                                    `week` string,
                                    season string,
                                    dt_status string comment "节假日,工作日,周末",
                                    is_workday int comment "1表示是 0 表示否",
                                    not_member_cnt INT COMMENT '普通用户租赁数量',
                                    member_cnt INT COMMENT '会员用户租赁数量',
                                    total_cnt INT COMMENT '总租赁数量'
)comment "时间主题"
partitioned by (dt_key string)
row format delimited fields terminated by ","
stored as orc
tblproperties ("compress"="snappy");



with a as (
    select
        date_format(ol.start_time,"yyyy-MM-dd") as dt,
        hour(ol.start_time) hr,
        dz.type,
        count(1) as cnt
    from ods_ride_log ol
             join dim_user_info_zip dz
                  on ol.userid = dz.userid
    group by date_format(ol.start_time,"yyyy-MM-dd"),dz.type,hour(ol.start_time)
    order by dt,hr
),
     b as (
         select
             dt,
             a.hr,
             type,
             cnt,
             week,
             week_cnt,
             status,
             is_workday_code
         from a join dim_date dd
                     on a.dt = dd.`date`
         order by dt,hr
     )
insert overwrite table cdm_user_time_topic
select
    c.dt,
    c.hr,
    c.week,
    case when month(c.dt) <= 3 then "春季"
         when month(c.dt) > 3 and month(c.dt) <=6 then "夏季"
         when month(c.dt) > 6 and month(c.dt) <=9 then "秋季"
         else "冬季" end,
    c.status,
    c.is_workday_code,
    c.cnt,
    d.cnt,
    c.cnt + d.cnt,
    "2024-01-01"
from (select * from b where type = "普通") c
         join (select * from b where type = "会员") d
              on c.dt = d.dt and c.hr = d.hr;

select * from cdm_user_time_topic;

drop table cdm_user_weather_topic;
create table cdm_user_weather_topic(
                                       dt string comment "日期",
                                       city_code int comment "城市编码用于查询天气",
                                       city_name string comment "城市名称",
                                       weather INT COMMENT '天气情况',
                                       temp FLOAT COMMENT '温度',
                                       hum FLOAT COMMENT '湿度',
                                       windspeed FLOAT COMMENT '风速',
                                       not_member_cnt INT COMMENT '普通用户租赁数量',
                                       member_cnt INT COMMENT '会员用户租赁数量',
                                       total_cnt INT COMMENT '总租赁数量'
)comment "天气主题"
partitioned by (dt_key string)
row format delimited fields terminated by ","
stored as orc
tblproperties ("compress"="gzip");


with a as (
    select
        date_format(ol.start_time,"yyyy-MM-dd") as dt,
        geohash(start_lat,start_lng,5) as geo,
        start_lng,
        start_lat,
        type
    from ods_ride_log ol
             join dim_user_info_zip dz
                  on ol.userid = dz.userid
),
     b as (
         select
             dt,
             code,
             district,
             type,
             count(1) as cnt
         from (
                  select
                      a.dt,
                      coalesce(dh.district_code,
                               get_json_object(location(start_lng,start_lat),"$.districtCode"),
                               get_json_object(location_baidu(cast(start_lng as double),cast(start_lat as double)),"$.adcode")
                          ) as code,
                      coalesce(dh.district,
                               get_json_object(location(start_lng,start_lat),"$.district"),
                               get_json_object(location_baidu(cast(start_lng as double),cast(start_lat as double)),"$.district")
                          ) as district,
                      a.type
                  from a
                           join dim_geo_hash dh
                                on a.geo = dh.geo_hash
                  )t group by dt, code,district,type),
     c as (
         select
             b.dt,
             b.code,
             b.district,
             dw.weather,
             dw.temp,
             dw.hum,
             dw.windspeed,
             b.type,
             count(1) as cnt
         from b
                  join dim_weather dw
                       on b.dt = dw.dt and b.code = dw.city_code
         group by b.type,b.dt,b.code,b.district,dw.weather,dw.temp,dw.hum,dw.windspeed
     )insert overwrite table cdm_user_weather_topic
select
    t1.dt,
    t1.code,
    t1.district,
    t1.weather,
    t1.temp,
    t1.hum,
    t1.windspeed,
    t1.type,
    t1.cnt,
    t2.cnt,
    t1.cnt + t2.cnt
from (select * from c where type = "普通") t1
         join (select * from c where type = "会员") t2
              on t1.dt = t2.dt;


-- ads
-- 用户信息特征
drop table ads_user_education_rate;
create table ads_user_education_rate(
                                        education string,
                                        user_cnt int,
                                        total_cnt int,
                                        rate decimal(10,3),
                                        percentage string
)comment "学历分布"
row format delimited fields terminated by ",";

with a as (
    select
        userid,
        education,
        sum(1) over () as total
    from cdm_user_info_topic
    group by userid, education
)
insert overwrite table ads_user_education_rate
select
    education,
    count(1),
    total,
    count(1) / total,
    concat(round((count(1) / total)* 100,1) ,"%")  --0.123 12.3
from a
group by education,total;

drop table ads_user_city_rate;
create table ads_user_city_rate(
                                   city_level string,
                                   user_cnt int,
                                   total_cnt int,
                                   rate decimal(4,3),
                                   percentage char(5)
)comment "城市分布"
row format delimited fields terminated by ",";

with a as (
    select
        userid,
        register_location,
        sum(1) over () as total
    from cdm_user_info_topic
    group by register_location,userid
)
insert overwrite table ads_user_city_rate
select
    dl.levle,
    count(1),
    total,
    count(1) / total,
    concat(round((count(1) / total) * 100,1),"%")
from a
         join dim_city_level dl
              on substr(a.register_location,1,length(a.register_location)-1) = dl.name
group by dl.levle,total;

drop table ads_user_age_rate;
create table ads_user_age_rate(
                                  age_range string,
                                  user_cnt int,
                                  total_cnt int,
                                  rate decimal(4,3),
                                  percentage char(5),
                                  avg_age decimal(4,2)
)comment "年龄分布"
row format delimited fields terminated by ",";

with a as (
    select
        userid,
        age_range,
        sum(1) over () as total,
        avg(age) over () as avg_age
    from cdm_user_info_topic
    group by userid, age_range,age
)
insert overwrite table ads_user_age_rate
select
    age_range,
    count(1),
    total,
    count(1) / total,
    concat(round((count(1) / total) * 100,1),"%"),
    avg_age
from a
group by age_range,total,avg_age;

drop table ads_user_gender_rate;
create table ads_user_gender_rate(
                                     gender string,
                                     user_cnt int,
                                     total_cnt int,
                                     rate decimal(4,3),
                                     percentage char(5)
)comment "性别分布"
row format delimited fields terminated by ",";

with a as (
    select
        userid,
        gender,
        sum(1) over () as total
    from cdm_user_info_topic
    group by userid, gender
)
insert overwrite table ads_user_gender_rate
select
    gender,
    count(1),
    total,
    count(1) / total,
    concat(round((count(1) / total) * 100,1),"%")
from a
group by gender,total;

drop table ads_user_type_rate;
create table ads_user_type_rate(
                                   usertype string,
                                   user_cnt int,
                                   total_cnt int,
                                   rate decimal(4,3),
                                   percentage char(5)
)comment "用户类型"
row format delimited fields terminated by ",";
-- over 分组之后想得到明细结果
with a as (
    select
        userid,
        usertype,
        sum(1) over () as total
    from cdm_user_info_topic
    group by userid, usertype
)
insert overwrite table ads_user_type_rate
select
    usertype,
    count(1),
    total,
    count(1) / total,
    concat(round((count(1) / total) * 100,1),"%")
from a
group by usertype,total;

drop table ads_user_job_rate;
create table ads_user_job_rate(
                                  job string,
                                  user_cnt int,
                                  total_cnt int,
                                  rate decimal(4,3),
                                  percentage char(5)
)comment "职业分布"
row format delimited fields terminated by ",";

with a as (
    select
        userid,
        job,
        sum(1) over () as total
    from cdm_user_info_topic
    group by userid, job
)
insert overwrite table ads_user_job_rate
select
    job,
    count(1),
    total,
    count(1) / total,
    concat(round((count(1) / total) * 100,1),"%")
from a
group by job,total;

drop table ads_user_salary_rate;
create table ads_user_salary_rate(
                                     salary string,
                                     user_cnt int,
                                     total_cnt int,
                                     rate decimal(4,3),
                                     percentage char(5)
)comment "收入分布"
row format delimited fields terminated by ",";

with a as (
    select
        userid,
        salary,
        sum(1) over () as total
    from cdm_user_info_topic
    group by userid, salary
)
insert overwrite table ads_user_salary_rate
select
    salary,
    count(1),
    total,
    count(1) / total,
    concat(round((count(1) / total) * 100,1),"%")
from a
group by salary,total;

drop table ads_user_cycle_speed;
create table ads_user_cycle_speed(
                                     dt string comment "计算时间",
                                     age_range string comment "年龄范围",
                                     gender string,
                                     bike_type string,
                                     speed string
)comment "骑行速度"
row format delimited fields terminated by ",";

drop table ads_user_cycle_distance;
create table ads_user_cycle_distance(
                                        dt string comment "计算时间",
                                        age_range string comment "年龄范围",
                                        gender string,
                                        bike_type string,
                                        distance string,
                                        max_distance string,
                                        min_distance string
)comment "骑行距离"
row format delimited fields terminated by ",";

insert overwrite table ads_user_cycle_distance
select
    cast(date_sub(`current_date`(),1) as string),
    age_range,
    gender,
    bike_type,
    concat(round(avg(distance),3)," KM"),
    concat(max(distance)," KM"),
    concat(min(distance)," KM")
from cdm_user_ride_topic
group by age_range,gender,bike_type,date_sub(`current_date`(),1)
union
select
    *
from ads_user_cycle_distance;

drop table ads_user_cycle_duration;
create table ads_user_cycle_duration(
                                        dt string comment "计算时间",
                                        age_range string comment "年龄范围",
                                        gender string,
                                        bike_type string,
                                        duration string,
                                        max_duration string,
                                        min_duration string
)comment "骑行时长"
row format delimited fields terminated by ",";

insert overwrite table ads_user_cycle_duration
select
    cast(date_sub(`current_date`(),1) as string),
    age_range,
    gender,
    bike_type,
    concat(round(avg(trip_duration),3)," S"),
    concat(max(trip_duration)," S"),
    concat(min(trip_duration)," S")
from cdm_user_ride_topic
group by age_range,gender,bike_type,date_sub(`current_date`(),1)
union
select
    *
from ads_user_cycle_duration;

drop table ads_user_cycle_frequence;
create table ads_user_cycle_frequence(
                                         week_cnt int comment "计算时间",
                                         age_range string comment "年龄范围",
                                         gender string,
                                         cnt_week int,
                                         cnt_month int,
                                         flag string comment "标记 当前数据是周还是月"
)comment "骑行频率"
row format delimited fields terminated by ",";


insert overwrite table ads_user_cycle_frequence
select
    weekofyear(start_time),
    age_range,
    gender,
    count(1),
    -1,
    "week"
from cdm_user_ride_topic
group by age_range,gender,weekofyear(start_time)
union
select
    month(start_time),
    age_range,
    gender,
    -1,
    count(1),
    "month"
from cdm_user_ride_topic
group by age_range,gender,month(start_time)
union
select
    *
from ads_user_cycle_frequence;

drop table ads_kaggle_hour;
create table ads_kaggle_hour(
                                hour string,
                                is_workday string,
                                not_member_cnt int,
                                member_cnt string,
                                total_cnt int
)row format delimited fields terminated by ",";

insert overwrite table ads_kaggle_hour
select
    `time`,
    `if`(is_workday=1,"工作日","非工作日"),
    cnt1,
    cnt2,
    cnt3
from(
        select
            `time`,
            is_workday,
            sum(not_member_cnt) cnt1,
            sum(member_cnt) cnt2,
            sum(total_cnt) cnt3
        from cdm_user_time_topic
        group by `time`,is_workday
        )a;

drop table ads_kaggle_workday;
create table ads_kaggle_workday(
                                   is_workday string,
                                   not_member_cnt int,
                                   member_cnt string,
                                   total_cnt int
)
    row format delimited fields terminated by "|";

insert overwrite table ads_kaggle_workday
select
    `if`(is_workday=1,"工作日","非工作日"),
    cnt1,
    cnt2,
    cnt3
from (
         select
             is_workday,
             sum(not_member_cnt) cnt1,
             sum(member_cnt) cnt2,
             sum(total_cnt) cnt3
         from cdm_user_time_topic
         group by is_workday
         )t1;


drop table ads_night_cycle;
create table ads_night_cycle(
                                dt string,
                                not_cnt int ,
                                cnt int ,
                                total_cnt int
)comment "夜间骑行占比"
row format delimited fields terminated by "|";

with a as (
    select * from cdm_user_time_topic where `time` >= 21 or `time` <=6
)
insert overwrite table ads_night_cycle
select
    date_format(dt,"yyyy-MM"),
    sum(not_member_cnt) as not_cnt,
    sum(member_cnt) as cnt,
    sum(total_cnt) as total_cnt
from a
group by date_format(dt,"yyyy-MM");

drop table ads_kaggle_season;
create table ads_kaggle_season(
                                  season string,
                                  not_member_cnt int,
                                  member_cnt string,
                                  total_cnt int
)row format delimited fields terminated by "|";

insert overwrite table ads_kaggle_season
select
    season,
    cnt1,
    cnt2,
    cnt3
from(
        select
            season,
            sum(not_member_cnt) cnt1,
            sum(member_cnt) cnt2,
            sum(total_cnt) cnt3
        from cdm_user_time_topic
        group by season
        )a;

drop table ads_kaggle_weather;
create table ads_kaggle_weather(
                                   weather string,
                                   not_member_cnt int,
                                   member_cnt string,
                                   total_cnt int
)row format delimited fields terminated by "|";

insert overwrite table ads_kaggle_weather
select
    weather,
    cnt1,
    cnt2,
    cnt3
from(
        select
            weather,
            sum(not_member_cnt) cnt1,
            sum(member_cnt) cnt2,
            sum(total_cnt) cnt3
        from cdm_user_weather_topic
        group by weather
        )a;

create table ads_kaggle_temp(
                                temp string,
                                not_member_cnt int,
                                member_cnt string,
                                total_cnt int
)row format delimited fields terminated by "|";


insert overwrite table ads_kaggle_temp
select
    temp ,
    cnt1,
    cnt2,
    cnt3
from(
        select
            temp,
            sum(not_member_cnt) cnt1,
            sum(member_cnt) cnt2,
            sum(total_cnt) cnt3
        from cdm_user_weather_topic
        group by temp
        )a;

create table ads_kaggle_hum(
                               hum string,
                               not_member_cnt int,
                               member_cnt string,
                               total_cnt int
)row format delimited fields terminated by "|";

insert overwrite table ads_kaggle_hum
select
    hum,
    cnt1,
    cnt2,
    cnt3
from(
        select
            hum,
            sum(not_member_cnt) cnt1,
            sum(member_cnt) cnt2,
            sum(total_cnt) cnt3
        from cdm_user_weather_topic
        group by hum
        )a;

create table ads_kaggle_windspeed(
                                     season string,
                                     not_member_cnt int,
                                     member_cnt string,
                                     total_cnt int
)row format delimited fields terminated by "|";

insert overwrite table ads_kaggle_windspeed
select
    windspeed,
    cnt1,
    cnt2,
    cnt3
from(
        select
            windspeed,
            sum(not_member_cnt) cnt1,
            sum(member_cnt) cnt2,
            sum(total_cnt) cnt3
        from cdm_user_weather_topic
        group by windspeed
        )a;

create table ads_kaggle_year(
                    dt string,
                    not_member_cnt int,
                    member_cnt string,
                    total_cnt int
)row format delimited fields terminated by "|";

insert overwrite table ads_kaggle_year
select
    date_format(dt,"yyyy-MM") as cnt,
    sum(not_member_cnt) cnt1,
    sum(member_cnt) cnt2,
    sum(total_cnt) cnt3
from cdm_user_time_topic
group by date_format(dt,"yyyy-MM")
order by cnt;

drop table ads_total;
create table ads_total(
    fields_explain string,
    cnt int
)comment "统计数据"
row format delimited fields terminated by ",";

insert overwrite table ads_total
select
    "订单总数",
    count(1)
from cdm_user_ride_topic
union
select
    "用户总数",
    count(1)
from cdm_user_info_topic
union
select
    "自行车总数",
    count(1)
from dim_bike_info;


drop table ads_last_10_day_order;
create table ads_last_10_day_order(
    dt string,
    cnt int
)comment "近十天骑行订单"
row format delimited fields terminated by ",";

insert overwrite table ads_last_10_day_order
select
    date_format(start_time,"yyyy-MM-dd"),
    count(1) as cnt
from cdm_user_ride_topic
where date_format(start_time,"yyyy-MM-dd")
between  date_sub(`current_date`(),11) and  date_sub(`current_date`(),1)
group by date_format(start_time,"yyyy-MM-dd");

drop table ads_last_10_day_register;
create table ads_last_10_day_register(
                  dt string,
                  cnt int
)comment "近十天注册用户"
row format delimited fields terminated by ",";

insert overwrite table ads_last_10_day_register
select
    date_format(register_dt,"yyyy-MM-dd"),
    count(1)
from cdm_user_info_topic
where date_format(register_dt,"yyyy-MM-dd")
between  date_sub(`current_date`(),11) and  date_sub(`current_date`(),1)
group by date_format(register_dt,"yyyy-MM-dd");

drop table ads_city_total;
create table ads_city_total(
           dt string,
           city string,
           cnt int
)comment "昨日各个城市订单数"
row format delimited fields terminated by ",";

-- json 路径 $..name 根下面所有符合条件的节点
with a as (
    select
        date_format(start_time,"yyyy-MM-dd") as dt,
        geohash(start_lng,start_lat,5) as geo,
        start_lng,
        start_lat
    from cdm_user_ride_topic
    where date_format(start_time,"yyyy-MM-dd") = date_sub(`current_date`(),1)
)
insert overwrite table ads_city_total
select
    dt,
    district,
    count(1)
from (
         select
             a.dt as dt,
             coalesce(dh.city,
                      get_json_object(location(start_lng,start_lat),"$..city"),
                      get_json_object(location_baidu(cast(start_lng as double),cast(start_lat as double)),"$..city")
                 ) as city
         from a
         join dim_geo_hash dh
         on a.geo = dh.geo_hash
)t group by dt,city;