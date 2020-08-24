#!/bin/bash
# set your JAVA_HOME
#export JAVA_HOME=/opt/cloudera/parcels/jdk8

# set your HADOOP_CONF_DIR
export HADOOP_CONF_DIR=/ideal/hadoop/hadoop/etc/hadoop

# set your FLINK_HOME
export FLINK_HOME=/ideal/hadoop/flink
export FLINK_PLUGINS_DIR=$FLINK_HOME/plugins
export FLINK_CONF_DIR=$FLINK_HOME/conf


# set your SPARK_HOME
export SPARK_HOME=/ideal/hadoop/spark