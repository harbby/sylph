title: 启动失败
---

```
FATAL: 17-03-27 15:45:52 [-:-] errno[0] logId[-] uri[-] user[-] refer[-] cookie[-]  [yog-ral] [cluster main][Config] config [/home/map/nodeui/conf/ral] load failed

AssertionError: [online] config error => [server]: expected undefined to be an array
    at validate (/home/map/nodeui/node_modules/node-ral/lib/config.js:122:45)
    at /home/map/nodeui/node_modules/node-ral/lib/config.js:141:9
    at Function._.map._.collect (/home/map/nodeui/node_modules/node-ral/node_modules/underscore/underscore.js:172:24)
    at loadRawConf (/home/map/nodeui/node_modules/node-ral/lib/config.js:134:7)
    at Object.load (/home/map/nodeui/node_modules/node-ral/lib/config.js:210:9)
    at Function.RAL.init (/home/map/nodeui/node_modules/node-ral/lib/ral.js:477:16)
    at module.exports (/home/map/nodeui/extension/tools/RAL.js:17:9)
    at /home/map/nodeui/kernel/middlewares/dispatcher/index.js:40:23
    at Array.forEach (native)
    at initCommonTools (/home/map/nodeui/kernel/middlewares/dispatcher/index.js:35:15)
```

这种 `ral` 配置问题通常是因为向后请求模块引用错误。本地开发环境引用 `node-ral` ，生产环境必须用 `yog-ral`。

### node-ral 与 yog-ral 的区别

