#!/bin/bash
cd ${0%/*}/..
MAIN_HOME=.

source etc/sylph/sylph-env.sh
#JAVA10_HOME
echo JAVA10_HOME=$JAVA10_HOME
export JAVA_HOME=$JAVA10_HOME
export PATH=$JAVA10_HOME/bin:$PATH
java -version

#stop 通用 启动脚本 mainClass 为进程坚持程序 必须唯一且可靠 否则请修改pid获取办法
mainClass=ideal.sylph.main.SylphMaster


for jar in $MAIN_HOME/lib/*.jar;
do
    CLASSPATH=$CLASSPATH:$jar
done

if [ -z $GRAPHX_OPTS ]; then
  GRAPHX_OPTS=-Xmx1G
fi

#HADOOP_CONF_DIR=/etc/hadoop/conf
#
exec java $GRAPHX_OPTS -cp lib/*: -Dconfig=etc/sylph/sylph.properties -Dlog4j.file=etc/sylph/sylph-log4j.properties $mainClass "$@"


#nohup $cmd > ${0%/*}/../logs/server.log 2>&1 &
#echo "Starting $mainClass,the pid is "$!
