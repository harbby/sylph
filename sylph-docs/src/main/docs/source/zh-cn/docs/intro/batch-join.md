title: 维表join
---
维表join在大数据中非常常见,通常我们需要在入库时对数据进行过滤和join打宽.
sylph sql现在已经支持简单的维表join,

### demo
下面将通过一个demo来演示sylph 的join功能.
该例子模拟读取实时json数据进行解析取出user_id和ip字段,
然后计算每个用户每5秒的`count(distinct key)`数,
接下来和mysql中的users表 进行join取出 name和city。
最后 Insert into 存储数据到外部
```sql
create function json_parser as 'ideal.sylph.runner.flink.udf.JsonParser';
create function row_get as 'ideal.sylph.runner.flink.udf.RowGet';

create source table topic1(
    key varchar,
    value varchar,     -- json
    event_time bigint
) with (
    type = 'ideal.sylph.plugins.flink.source.TestSource'
);

-- 定义数据流输出位置
create sink table print_table_sink(
    uid varchar,
    name varchar,
    city varchar,
    cnt long,
    window_time varchar
) with (
    type = 'console',   -- print console
    other = 'demo001'
);

-- 定义维表
create batch table users(
    id varchar,
    name varchar,
    city varchar
) with (
    type = 'mysql',   -- print console
    userName = 'demo',
    password = 'demo',
    url = 'jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false'
    -- query = 'select * from users where ...'  --可以下推谓词
);

-- 描述数据流计算过程
insert into print_table_sink
with tb1 as (
    select key, row_get(rowline,0) as uid , row_get(rowline,1) as ip, event_time, proctime
    from topic1 , LATERAL TABLE(json_parser(`value`,'user_id,ip')) as T(rowline) 
),tb2 as (
    select uid,
    count(distinct key) as cnt,
    cast(TUMBLE_START(proctime,INTERVAL '5' SECOND) as varchar)|| '-->' 
    || cast(TUMBLE_END(proctime,INTERVAL '5' SECOND) as varchar) AS start_time
    from tb1 where uid is not null
    group by uid,TUMBLE(proctime,INTERVAL '5' SECOND)
) 
select tb2.uid, users.name ,users.city, tb2.cnt, tb2.start_time 
from tb2 left join users on tb2.uid = users.id
having 1=1
```
    
### 注意事项
- 1, 维表join时 为了解析方便,请尽量将维表放到右边
- 2, 只支持left join 和 Inner join(维表在右边时), 详细原因可以参考: [http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#support-matrix-for-joins-in-streaming-queries](join)
- 3  `on` 只支持and(equals) 例如: `条件1 and 条件2 and 条件3`
- 4  对于条件只支持equals(`=`) 例如: `on tb1.a1=tb2.id and ...`
- 5  对于equals条件暂时不支持常量, 例如: `on tb1.a1 = 'sylph'` 目前是不支持的, 原因是涉及到谓词下推问题
- 6  不支持join where 原因如上 因为涉及到谓词下推问题
- 7  完整支持 having语句

### 关于维表插件 目前只实现了mysql
实现非常简单,请参考 sylph-mysql/ideal.sylph.plugins.mysql.MysqlAsyncFunction.java
如果有用到redis或者别的 可参考这个进行简单实现,或参考`进阶`中开发指南

### 关于缓存问题
MysqlAsyncFunction 采用LRU缓存策略, 使用的本地缓存. 如果想使用分布式缓存,可以自行修改非常简单.


### other
- 关于json 解析采用的udtf来实现的,总体上因为calcite语法原因 对比hive显得不够优雅
但在这种方案不影响性能 切非常灵活
- 上面示例mysql 建表语句如下:
```mysql
-- ----------------------------
-- Table structure for users
-- ----------------------------
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` varchar(10) NOT NULL,
  `name` varchar(20) NOT NULL,
  `city` char(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of users
-- ----------------------------
INSERT INTO `users` VALUES ('1', 'h123', '123');
INSERT INTO `users` VALUES ('2', 'p123', '123');
INSERT INTO `users` VALUES ('4', '小王', 'dikd3939');
INSERT INTO `users` VALUES ('uid_1', 'test', 'test');
INSERT INTO `users` VALUES ('uid_5', 'sylph', 'demo');
``` 