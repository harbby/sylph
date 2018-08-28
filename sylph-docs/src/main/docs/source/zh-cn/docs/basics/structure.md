title: 目录结构
---

在[快速入门](../intro/quickstart.md)中，对sylph有了初步的印象，接下来我们简单了解下安装目录约定规范。

### 1. 框架目录结构
![framework]

### 2. 业务模块结构
![business]

[framework]: ../../../images/logo.png
[business]: ../../../images/logo.png

```bash
sylph-bin-${version}
├── bin (启动脚本)
├── etc 
│   └── sylph (配置)
├── etl-plugins (pipeline插件,如下有两个插件包)
│   ├── flink-node-plugin
│   └── spark-node-plugin
├── jobs (job保存目录)
│   ├── a2
│   ├── spark_test1
│   └── sql_test1
├── lib (核心依赖)
├── logs
├── modules (执行器 如有flink,spark执行器)
│   ├── sylph-runner-flink
│   │   └── lib
│   └── sylph-runner-spark
│       └── lib
└── webapps (webUI)
    ├── css
    ├── fonts
    ├── img
    ├── js
    ├── ...
```

如上，整个sylph的安装目录：
