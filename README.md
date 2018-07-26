# Sylph [![Build Status](http://img.shields.io/travis/harbby/sylph.svg?style=flat&branch=master)](https://travis-ci.org/harbby/sylph)
The sylph is stream and batch Job management platform. 
The sylph core idea is to build distributed applications through workflow descriptions.
Support for 
* spark1.x Spark-Streaming
* spark2.x Structured-Streaming 
* flink stream
* batch job


## Show
[![loading...](https://raw.githubusercontent.com/harbby/harbby.github.io/master/logo/sylph/briefing.gif)](https://travis-ci.org/harbby/sylph)

## Building
sylph builds use Maven and requires Java 8.

```
# Build and install distributions
./gradlew clean assemble
```
## Running Presto in your IDE
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
* Working directory: $MODULE_DIR$
* Use classpath of module: sylph-main
 
## Useful mailing lists
1. yezhixinghai@gmail.com - For discussions about code, design and features