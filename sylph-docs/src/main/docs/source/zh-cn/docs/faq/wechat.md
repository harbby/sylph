title: 微信授权
---

后端服务通常对微信授权相关接口做了IP限制，我们联调测试时候需要关闭其限制。

### 内网接口

- /hongbao/getmobilebyopenid
- /hongbao/addmobileopenid
- /hongbao/updatemobilebyopenid

### 关闭内网限制

    /home/map/odp_cater/app/hongbao/actions/api
    
    注释：$this->checkIp = true;

### 附微信授权原理流程图

![wechat](http://younth.coding.me/static/wechat-auth.png)