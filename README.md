# Sylph [![Build Status](https://travis-ci.com/harbby/sylph.svg?branch=master)](https://travis-ci.com/harbby/sylph)
[![license](https://img.shields.io/badge/license-apache_v2-groon.svg)]()
[![language](https://img.shields.io/badge/language-java_17-green.svg)]()
[![os](https://img.shields.io/badge/os-Linux_macOS-blue.svg)]()

Welcome to Sylph !

Sylph is Streaming Job Manager. 

Sylph uses SQL Query to describe calculations and bind multiple source(input)/sink(output) to visually develop and deploy streaming applications.
Through Web IDE makes it easy to develop, deploy, monitor streaming applications and analyze streaming application behavior at any time.  
Sylph has rich source/sink support and flexible extensions to visually develop and deploy stream analysis applications and visualized streaming application lifecycle management.

The Sylph core is to build distributed applications through workflow descriptions.
Support for 
* Spark-Streaming (Spark1.x)
* Structured-Streaming (Spark2.x)
* Flink Streaming

## License
```
Copyright (C) 2018 The Sylph Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## StreamingSql
```sql
create function get_json_object as 'com.github.harbby.sylph.runner.flink.runtime.UDFJson';

create source table topic1(
    _topic varchar,
    _key varchar,
    _partition integer,
    _offset bigint,
    _message varchar,
    ip varchar extend '$.conntent.ip',                 -- json path
    event_time varchar extend '$.conntent.event_time'  -- json path
) with (
    type = 'kafka08',
    kafka_topic = 'event_topic',
    auto.offset.reset = latest,
    kafka_broker = 'localhost:9092',
    kafka_group_id = 'test1',
    zookeeper.connect = 'localhost:2181'
);

-- 定义数据流输出位置
create sink table event_log(
    key varchar,
    user_id varchar,
    offset bigint
) with (
    type = 'kudu',
    kudu.hosts = 'localhost:7051',
    kudu.tableName = 'impala::test_kudu.log_events',
    kudu.mode = 'INSERT',
    batchSize = 5000
);

insert into event_log
select _key,get_json_object(_message, 'user_id') as user_id,_offset 
from topic1
```

## UDF UDAF UDTF
The registration of the custom function is consistent with the hive
```sql
create function get_json_object as 'com.github.harbby.sylph.runner.flink.runtime.UDFJson';
```

## Building
sylph builds use Gradle and requires Java 8.    
Also if you want read a chinese deploy docs,[中文部署文档](sylph-docs/src/main/docs/source/zh-cn/docs/intro/deploy.md) 
may can help you.
```
# Build and install distributions
./gradlew clean assemble dist
```
## Running Sylph in your IDE
After building Sylph for the first time, you can load the project into your IDE and run the server. Me recommend using IntelliJ IDEA.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Sylph makes use of several Java 8 language features
* HADOOP_HOME(2.6.x+) SPARK_HOME(2.4.x+) FLINK_HOME(1.7.x+)

Sylph comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: com.github.harbby.sylph.main.SylphMaster
* VM Options: -Dconfig=etc/sylph/sylph.properties -Dlogging.config=etc/sylph/logback.xml
* ENV Options: FLINK_HOME=<your flink home>
               HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
* Working directory: sylph-dist/build
* Use classpath of module: sylph-main
 
## Useful mailing lists
1. yezhixinghai@gmail.com - For discussions about code, design and features
2. lydata_jia@163.com -  For discussions about code, design and features
3. jeific@outlook.com - For discussions about code, design and features

## Getting Help
* Send message to [Google Group](https://groups.google.com/forum/#!forum/sylph-streaming)
* Add QQ Group: 438625067
