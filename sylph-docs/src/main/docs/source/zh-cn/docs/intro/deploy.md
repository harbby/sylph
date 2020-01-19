<center><h1>sylph部署</h1></center>


## 一些准备

- **JDK**

  首先，请确保JDK版本在1.8以上，查看Java版本  ***java -version***；如果没有安装，请先下载并安装jdk 1.8+

- **clone**

  sylph基于flink的版本，目前主要两个大版本 flink 1.7.2 和 flink 1.9

  1. 如果您想稳定运行使用sylph，建议使用基于flink1.7的v1.6分支，使用以下命令即可：

     ```shell
     git clone https://github.com/harbby/sylph/tree/v1.6
     ```

  2. 如果您想体验flink1.9的魅力，可以直接使用master分支，使用以下命令即可：

     ```shell
     git clone https://github.com/harbby/sylph/tree/master
     ```

- **Flink**

  请下载Flink 1.7.2 with Scala 2.11 或者Flink 1.9 with Scala 2.12，这由您clone的sylph的版本决定。

  ***注意：flink以及下面的hadoop spark，只需要下载包解压即可，如果使用的是local模式，并不需要专门搭建或者启动任何的hadoop、spark或者flink的环境；假如您使用的是yarn模式，则需要yarn集群。***

- **Spark**

  请下载spark2.4.x+ with-hadoop-xxx （后面的版本由您下载的hadoop版本决定） 

- **Hadoop**

  请下载hadoop 2.6以上版本。

做好了准备工作，我们开始编译sylph。

## 编译

进入到clone后的sylph主目录下

***注：***首次编译一般耗时较长，如果想跳过一些java-docs代码的编译，可编辑主目录下***build.gradle***文件，注释以下内容即可：

```java
//  task javadocJar(type: Jar, dependsOn: javadoc) {
//    classifier = 'javadoc'
//    from javadoc.destinationDir
//    //javadoc.failOnError = false
//  }

  artifacts {
    archives sourcesJar
//    , javadocJar
  }
```

- Mac os，Unix环境下在项目根目录下执行

  ```shell
  ./gradlew clean assemble
  ```

- windows环境下执行

  ```shell
  gradlew.bat clean assemble
  ```

耐心等待编译完成，即可进行下一步运行了。

## 运行

请注意，尽管sylph支持Windows环境下的调试和运行，如果您想更好的使用sylph，建议在Unix环境下运行，以期得到更好的体验，下面将介绍两种运行方式。

### 命令行

- **解压**

  编译完成后，解压sylph-dist/build/distributions目录下的sylph-xxx.tgz文件,得到sylph-xxx目录

  ***注意：以下操作如果不做特殊说明，都是在sylph-xxx目录下做操作***

- **修改sylph.properties**

  打开etc/sylph/sylph.properties

  ```yaml
  # server web port
  web.server.port=8092
  # metadata path
  server.metadata.path=./data
  # job working dir
  server.jobstore.workpath=./jobs
  # job runtime mode, yarn or local
  job.runtime.mode=local
  ```

  可根据自己的需要修改配置，修改完后保存即可。

- **修改sylph-env.sh**

  ```shell
  #!/bin/bash
  # set your JAVA_HOME
  #export JAVA_HOME=/opt/cloudera/parcels/jdk8
  
  # set your HADOOP_CONF_DIR
  export HADOOP_CONF_DIR=/ideal/hadoop/hadoop/etc/hadoop
  
  # set your FLINK_HOME
  export FLINK_HOME=/ideal/hadoop/flink
  
  # set your SPARK_HOME
  export SPARK_HOME=/ideal/hadoop/spark
  ```

  配置说明

  - **HADOOP_CONF_DIR**

    hadoop conf全路径

    如果您使用的是local模式，只需要将解压后的hadoop etc/hadoop路径拷贝过来即可；

    如果您使用的是yarn模式，则这里是yarn集群hadoop conf配置文件所在目录。

    ***注意：如果因为某些原因，sylph无法部署在包含yarn hadoop配置的机器，也可将集群的hadoop配置直接拷贝到sylph所在机器上，并修改配置指向。这种情况下，请确保sylph所在机器可以成功访问您的集群。***

  - **FLINK_HOME**

    flink路径， 指向flink根目录

  - **SPARK_HOME** 

    spark路径，指向spark根目录

- **运行**

  修改完配置后，执行以下命令即可运行

  ```shell
  ./bin/launcher start
  ```

  日志文件为：logs/INFO.log

### IDEA 运行

- **导入项目**

  将sylph导入到idea中，在进行下一步之前，请确保一些环境信息，如jdk和java编译器必须是8等，具体请看：

  [running sylph in your idea](https://github.com/harbby/sylph/tree/master#running-sylph-in-your-ide)

- **Run/Debug Configurations**

  新建一个Application的run conf，进行配置的填写

  相关配置说明

  - **Main Class:** ideal.sylph.main.SylphMaster
  - **VM Options:** -Dconfig=etc/sylph/sylph.properties -Dlogging.config=etc/sylph/logback.xml
  - **Program arguments:** 如命令行运行中修改sylph-env.sh，将flink等的环境配置进来，以";"分割
  - **Working directory:** 目录到编译后的 sylph-dist/build目录下，即 $clone后的sylph目录/sylph-dist/build，全路径
  - **Environment variables：**与Program arguments相同
  - **Use classpath of module:** sylph-main
  - **JRE：**1.8

- **Run**

  运行配置好的application即可

至此，部署顺利完成，如果在部署或者使用过程中可以提[issues](https://github.com/harbby/sylph/issues)或者通过[联系我们](https://github.com/harbby/sylph/tree/master#getting-help)。希望sylph可以让您愉快

## 附录







