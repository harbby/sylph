title: 主要工作
---

通过PM2拆分服务，确保不同服务相互不受影响。

### 1. 改造现有`pm2.json`，支持多app启动

根据当前公有云Node集群现状（机器均为24核），拆分为两个独立服务：

- `nuomi`: 承载糯米组件流量，端口为`8197`，实例数为14
- `h5`: 承载所有h5模块渲染流量，端口为`8198`,实例数为10

```
{
    "apps": [{
        "exec_interpreter": "node",
        "name": "nuomi",
        "exec_mode": "cluster_mode",
        "instances": "14",
        "max_memory_restart": "500M",
        "merge_logs": false,
        "script": "app.js",
        "ignoreWatch": ["pm2", "log", "node_modules"],
        "env": {
            "NODE_ENV": "production",
            "PM2_GRACEFUL_TIMEOUT": 1000,
            "PORT": 8197,
            "DEBUG": true
        },
        "env_dev": {
            "NODE_ENV": "development",
            "DEBUG": true
        },
        "env_test": {
            "NODE_ENV": "qa"
        }
    }, {
        "exec_interpreter": "node",
        "name": "h5",
        "exec_mode": "cluster_mode",
        "instances": "10",
        "max_memory_restart": "500M",
        "merge_logs": false,
        "script": "app.js",
        "ignoreWatch": ["pm2", "log", "node_modules"],
        "env": {
            "NODE_ENV": "production",
            "PM2_GRACEFUL_TIMEOUT": 1000,
            "PORT": 8198,
            "DEBUG": true
        },
        "env_dev": {
            "NODE_ENV": "development",
            "DEBUG": true
        },
        "env_test": {
            "NODE_ENV": "qa"
        }
    }]
}

```

### 2. 改造启动脚本，支持业务APP独立重启

    nuomi_restart.sh
    h5_restart.sh

命令改造：

    bin/control start  // 开启所有服务
    bin/control stop appname // 关闭某个服务（h5、nuomi）
    bin/control delete appname // 删除某个服务（h5、nuomi）
    bin/control restart appname // 重启某个服务
    bin/control reload appname // 0宕机重启，后续上线重启采用reload方案
    
![reload](http://younth.coding.me/static/nodeui/reload.jpeg)

## 上线步骤

### 1. 上线NodeUI服务

- 上线前先确定线上的服务进程名称

- 涉及底层上线，上线前需要先stop/delete掉老的进程，为确保服务稳定，采用先stop，然后上线新的隔离方案，回归正常后，delete老的服务进程。

- 需要review方案，或者直接OP操作删除老的进程。
- 增加上线模块
- 绑定上线重启脚本



### 2. 上线ng rewrite 端口

- 配置新的vip服务
- 修改rewrite

```
/home/work/odp_cater/webserver/conf/vhost/
```

## 重点关注

- 组件业务 
- h5业务 
- 日志（pm2+业务，关注日志是否会错乱）
- 服务稳定（Node进程状态）

## 回滚方案

由于目前公有云回滚可能有坑，回滚过程进程应该不会被杀掉，可能有残留进程，需要杀掉。所以最佳回归方式是**覆盖上线**。涉及的模块：

- fly
- runtime
