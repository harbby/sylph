title: ETL 任务介绍
---

streamSql描述流计算非常简单快捷,但是某些时候有些业务逻辑不便于sql化, 也不需要用到window等复杂机制
需要通过代码实现更复杂的数据数据处理,但希望整个数据流处理变的简单。那么这时使用流式ETL是非常合适的

### node
sylph对etl过程进行如下三个环节(node)抽象,将一切数据流活动都用如下三个环节表示,并通过`箭头`表示数据流向
通过一个flow来描述整个流计算过程
- source
    (该算子实现如何将数据流接入系统)
- transform
    (该算子实现如何将数据流进行转换)
- sink
    (该算子实现如何将数据流输到外部系统中)
    
### flow
![job_flow]

如上图通过 flow描述了一个实时etl过程,实例为: kafka->业务处理->hdfs

[job_flow]: ../../../images/sylph/job_flow.png