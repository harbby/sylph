title: Node.js调试
---

## 线下调试
    
### 需求

- http请求断点调试
- node-ral 向后请求查看
- 启动调试


## 版本

    node v4.2.4
    Node Inspector v0.12.5

## node-inspector

```
npm i -g node-inspector

npm run debug(node --debug app.js)

node-inspector --web-port=5006

```

> 建议配合 `supervisor` 使用,支持node 自动重启。
    
## devtool

> 建议大家尝试，取代node-inspector

    https://github.com/Jam3/devtool

## 线上—日志

    线上通过日志排查 access nodeui等
    
## 远程调试


## 错误捕获

```js
'use strict';

const Http = require('http');

const server = Http.createServer((req, res) => {
  const promise = new Promise((resolve, reject) => {
    let result = '';
    req.on('data', (data) => {
      result += data;
    });

    req.once('end', () => {
      const obj = JSON.parse(result);
      resolve(obj);
    });
  });

  promise.then((obj) => {
    res.writeHead(200);
    // 这里会报错
    res.end(obj.foo.bar);
  }).catch((reason) => {
    res.writeHead(500);
    res.end();
    console.error(reason);
    // 抛错后退出程序
    process.abort()
  });
});

server.listen(8080, () => {
  console.log('listening at http://localhost:8080');
});

```
    // post请求
    curl -X POST http://localhost:8080 -d '{"Hi": "world"}'