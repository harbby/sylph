title: 快速入门 
---

下面将以StreamSql为实例，一步步地搭建出一个 分布式流计算应用，让你能快速的入门 SYLPH。

> StreamSql是完全通过类sql来描述整个流计算的过程。主要需要描述: 数据源如何接入、如何计算、如何输出到外部存储; 
例如计算每分钟的pv; 每5秒更新一次最近一分钟的uv。

### demo1
下面例子演示将kafka topic `TP_A_1,TP_A_2`的数据实时写入mysql表`mysql_table_sink`中

**注意: mysql中表`mysql_table_sink`需要提前自行创建好,并且字段类型要和实际兼容**
```sql
-- 定义数据流接入 
create source table topic1(
    _topic varchar,
    _key varchar,
    _message varchar,
    _partition integer,
    _offset bigint
) with (
    type = 'kafka',
    kafka_topic = 'TP_A_1,TP_A_2',
    "auto.offset.reset" = latest,
    kafka_broker = 'localhost:9092',
    kafka_group_id = 'streamSql_test1'
);
-- 定义数据流输出位置
create sink table mysql_table_sink(
    a1 varchar,
    a2 varchar,
    event_time bigint
) with (
    type = 'mysql',   -- ideal.sylph.plugins.flink.sink
    userName = 'demo',
    password = 'demo',
    url = 'jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false',
    other = 'demo001'
);
-- 描述数据流计算过程
insert into mysql_table_sink
select _topic,`_message`,cast(_offset as bigint) from topic1 where _key is not null
```

### demo2
下面的例子 演示如何计算topic `TP_A_1`每分钟的uv
```sql
create source table topic1(
    _topic varchar,
    _key varchar,
    _message varchar,
    _partition integer,
    _offset bigint
) with (
    type = 'kafka',
    kafka_topic = 'TP_A_1',
    "auto.offset.reset" = latest,
    kafka_broker = 'localhost:9092',
    kafka_group_id = 'streamSql_test1'
);

create sink table mysql_uv_table_sink(
    uv bigint,
    cnt_time date
) with (
    type = 'mysql',   -- ideal.sylph.plugins.flink.sink
    userName = 'demo',
    password = 'demo',
    url = 'jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false',
    other = 'demo001'
);

with tb13 as (SELECT proctime
    ,row_get(rowline,0)as user_id
    FROM cdn_c13, LATERAL TABLE(json_parser(_message,'user_id')) as T(rowline) 
    where cast(row_get(rowline,0) as varchar) is not null
)
insert into mysql_table_sink
select 
count_distinct(user_id) as uv
,TUMBLE_START(proctime,INTERVAL '60' SECOND) AS window_start 
FROM tb13 GROUP BY user_id,TUMBLE(proctime,INTERVAL '60' SECOND)
```

### strteamSql设计器
![strteamSql]

[strteamSql]: ../../../images/sylph/strteam_sql.png