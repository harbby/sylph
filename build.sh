#!/bin/bash

#JAVA10_HOME
#echo JAVA10_HOME=$JAVA10_HOME
#export JAVA_HOME=$JAVA10_HOME
#export PATH=$JAVA10_HOME/bin:$PATH
java -version

./gradlew -v

./gradlew clean checkstyle licenseMain licenseTest assemble test jacocoTestReport "$@"
