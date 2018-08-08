package ideal.sylph.plugins.flink.sink

import ideal.sylph.etl.Row
import ideal.sylph.etl.api.RealTimeSink

class PrintSink extends RealTimeSink {
  /**
    * partition级别的初始化
    **/
  override def open(partitionId: Long, version: Long): Boolean = true

  /**
    * line 级别的 需要注意线程安全问题
    **/
  override def process(value: Row): Unit = {
    println(value.mkString())
  }

  /**
    * partition级别的资源释放
    **/
  override def close(errorOrNull: Throwable): Unit = {}

  override def driverInit(optionMap: java.util.Map[String, Object]): Unit = {
  }
}
