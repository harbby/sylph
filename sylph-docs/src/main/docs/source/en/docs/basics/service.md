title: REST Service
---

SYLPH 中，使用restful风格接口:

- 后端服务配置统一管理
- 封装异常处理、超时重试，提升系统稳定性
- 封装日志，便于线上问题追查
- 抽象请求协议、数据格式与数据编码，统一接口

## 主要特征

- compile
- proxy

## 使用 Service

接口在`sylph-controller`模块中管理,主要分布在`ideal.sylph.controller.action`包下:

```java
    @javax.inject.Singleton
    @Path("/stream_sql")
    public class StreamSqlResource
    {
    ...
        
        @POST
        @Path("save")
        @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON})
        @Produces({MediaType.APPLICATION_JSON})
        public Map saveJob(@Context HttpServletRequest request){
            ...
        }
```


