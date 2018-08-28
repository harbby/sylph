title: NodeUI与PHP服务通信负载均衡策略
---

### 1. roundrobin

```
RoundRobinBalance.prototype.fetchServer = function (balanceContext) {
    var servers = balanceContext.reqIDCServers;
    if (servers.length === 1) {
        return servers[0];
    }
    balanceContext.lastRoundRobinID = balanceContext.lastRoundRobinID || 0;
    balanceContext.lastRoundRobinID++;
    if (balanceContext.lastRoundRobinID < 0 || balanceContext.lastRoundRobinID >= servers.length) {
        balanceContext.lastRoundRobinID = 0;
    }
    logger.trace(
        ['RoundRobinBalance fetchServer RoundRobinID=', balanceContext.lastRoundRobinID, ' ServiceID=',
            balanceContext.serviceID
        ].join('')
    );
    return servers[balanceContext.lastRoundRobinID];
};
```

### 2. random


```
RandomBalance.prototype.fetchServer = function (balanceContext) {
    var servers = balanceContext.reqIDCServers;
    if (servers.length === 1) {
        return servers[0];
    }
    // Math.random takes 250ms on first call
    var index = Math.floor(Math.random() * servers.length);
    logger.trace('RandomBalance fetchServer index=' + index + ' ServiceID=' + balanceContext.serviceID);
    // TODO add server filter
    return servers[index];
};

```

### 3. hashring

```
HashringBalance.prototype.fetchServer = function (balanceContext, conf, prevBackend) {
    if (conf.balanceKey === undefined || conf.balanceKey === null) {
        throw new Error('balanceKey is needed when using consistent hashing');
    }
    var servers = balanceContext.reqIDCServers;
    if (servers.length === 1) {
        return servers[0];
    }
    if (!balanceContext.hashring) {
        balanceContext.hashring = this.createHashring(balanceContext.reqIDCServers);
    }
    var ringIndex = bs(balanceContext.hashring, {
        index: null,
        value: this.generateHash(conf.balanceKey)
    }, function (a, b) {
        return a.value - b.value;
    });
    if (ringIndex < 0) {
        ringIndex = -ringIndex - 1;
    }
    ringIndex = Math.min(ringIndex, balanceContext.hashring.length - 1);
    var index = balanceContext.hashring[ringIndex].index;
    logger.trace('RandomBalance fetchServer index=' + index + ' ServiceID=' + balanceContext.serviceID);
    return servers[index];
};

```
