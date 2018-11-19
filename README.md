# Sylph [![Build Status](http://img.shields.io/travis/harbby/sylph.svg?style=flat&branch=master)](https://travis-ci.org/harbby/sylph)
The Sylph is Stream Job management platform. 
The Sylph core idea is to build distributed applications through workflow descriptions.
Support for 
* spark1.x Spark-Streaming
* spark2.x Structured-Streaming 
* flink stream

## StreamSql
```sql
create function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';

create source table topic1(
    key varchar,
    message varchar,
    event_time bigint
) with (
    type = 'ideal.sylph.plugins.flink.source.TestSource'
);

-- 定义数据流输出位置
create sink table event_log(
    key varchar,
    user_id varchar,
    event_time bigint
) with (
    type = 'hdfs',   -- write hdfs
    hdfs_write_dir = 'hdfs:///tmp/test/data/xx_log',
    eventTime_field = 'event_time',
    format = 'parquet'
);

insert into event_log
select key,get_json_object(message, 'user_id') as user_id,event_time 
from topic1
```

## UDF UDAF UDTF
The registration of the custom function is consistent with the hive
```sql
create function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';
```

## StreamETL 
Support `flink-stream` `spark-streaming` `spark-structured-streaming(spark2.2x)`

[![loading...](https://raw.githubusercontent.com/harbby/harbby.github.io/master/logo/sylph/job_flow.png)](https://travis-ci.org/harbby/sylph)


## Building
sylph builds use Gradle and requires Java 8.
```
# Build and install distributions
./gradlew clean assemble
```
## Running Sylph in your IDE
After building Sylph for the first time, you can load the project into your IDE and run the server. Me recommend using IntelliJ IDEA.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Sylph makes use of several Java 8 language features
* HADOOP_HOME(2.6.x+) SPARK_HOME(2.3.x+) FLINK_HOME(1.5.x+)

Sylph comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: ideal.sylph.main.SylphMaster
* VM Options: -Dconfig=etc/sylph/sylph.properties -Dlog4j.file=etc/sylph/sylph-log4j.properties
* ENV Options: FLINK_HOME=<your flink home>
               HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
* Working directory: sylph-dist/build
* Use classpath of module: sylph-main
 
## Useful mailing lists
1. yezhixinghai@gmail.com - For discussions about code, design and features
2. lydata_jia@163.com -  For discussions about code, design and features
## Help
We need more power to improve the view layer. If you are interested, you can contact me by email.

## Other
* sylph被设计来处理分布式实时ETL,实时StreamSql计算,分布式程序监控和托管.
* 加入QQ群 438625067