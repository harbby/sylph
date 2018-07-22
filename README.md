# Sylph [![Build Status](http://img.shields.io/travis/harbby/sylph.svg?style=flat&branch=master)](https://travis-ci.org/harbby/sylph)
The sylph is stream framework

## Building
sylph builds use Maven and requires Java 10 or higher.

```
# Build and install distributions
./gradlew clean assemble

# Clean the build
./gradlew clean
```
vm
-Dconfig=etc/sylph/sylph.properties
-Dlog4j.file=etc/sylph/sylph-log4j.properties

env
FLINK_HOME=/ideal/hadoop/flink
HADOOP_CONF_DIR=/ideal/hadoop/hadoop/etc/hadoop
 
## Useful mailing lists
1. yezhixinghai@gmail.com - For discussions about code, design and features