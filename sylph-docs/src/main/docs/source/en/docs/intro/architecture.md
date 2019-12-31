## 架构说明

### 架构图

![sylph架构图](https://github.com/JFanZhao/sylph-1/blob/master/sylph-docs/src/main/docs/source/en/docs/intro/sylph%E6%9E%B6%E6%9E%84%E5%9B%BE.png)

- **Running Environment：** 任务运行环境，目前支持local和yarn两种。由 job.runtime.mode 决定。
- **Container：** Container是任务提交给运行环境的单元，包含LocalContainer和YarnContainer两种，由 job.runtime.mode 决定；LocalContainer即任务运行在本地，可以在本地测试使用（支持Mac，Linux以及Windows环境）；YarnContainer即提交到yarn环境下环境运行，yarn环境在sylph-env.sh中设置。由Container 向发布任务到运行环境、停止任务运行、获取任务运行状态等。Container由JobManager创建，并将其加入运行队列中，进行调度。
- **JobEngine：** JobEngine会对用户提交的SQL进行构建和编译，倘若失败，也即意味着用户的SQL不符合Loris某些规范，只有JobEngine顺利执行后的任务，才会创建Container并进入任务队列，调度。
  JobEngine主要包含两个功能，其一：将用户提交的任务转换解析（Flow->Job->Graph）;其二则是，使用具体的Engine对转换后的Job进行编译。  
  JobEngine主要有flink 1.7.2 和spark 2.X ，每种引擎又包含多个执行引擎，具体类型如下（界面中可以选择相应的引擎）：  
  **Flink Engine**
  FlinkMainClassEngine：Main Class Engine，上传带main方法的jar包，目前界面不可选；   
  FlinkStreamEtlEngine：任务流实现，目前界面不支持；   
  FlinkStreamSqlEngine：SQL Engine，用户通过页面选择，并编写SQL提交。  
  **Spark Engine**  
  StreamEtlEngine：Spark Streaming 任务流实现，目前界面不支持；   
  Stream2EtlEngine：Structured Streaming 任务流实现，目前界面不支持；   
  SparkSubmitEngine：Spark Main Class Engine，上传带main方法的jar包，目前界面不可选；   
  SparkStreamingSQLEngine：Spark Streaming SQL Engine，用户通过页面选择，并编写SQL提交   
  StructuredStreamingSQLEngine：Structured Streaming SQL Engine，用户通过页面选择，并编写SQL提交   
- **JobManager**：JobManager是用户和底层的中转站，用户通过portal编写任务，提交给JobManager，JobManager负责任务的提交、启动、停止以及监控等。   
  JobManager主要包含如下几个模块   
  JobStore：负责任务配置、DDL以及状态信息等的持久化；   
  JobSchedulePool：任务调度池，需要启动的任务，首先进入调度池，等待调度；   
  Containers：即每个任务的Container，进入调度池的即是Container；   
  JobEngineManger：负责编译的Engine；   
  JobMonitor：监控任务的运行状态，重新拉起失败的任务。   
- **Portal**：用户操作界面，任务开发者通过页面创建和维护任务   