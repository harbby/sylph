#!/bin/bash

#JAVA10_HOME
#echo JAVA10_HOME=$JAVA10_HOME
#export JAVA_HOME=$JAVA10_HOME
#export PATH=$JAVA10_HOME/bin:$PATH
java -version

./gradlew -v
cd /home/admin/sylph/ && git pull origin dev_20180829_merger
#sh /home/admin/sylph/sylph-dist/build/bin/launcher stop

./gradlew clean assemble install

#./gradlew clean checkstyle assemble test "$@"

sh /home/admin/sylph/sylph-dist/build/bin/launcher restart

#./gradlew clean assemble install
