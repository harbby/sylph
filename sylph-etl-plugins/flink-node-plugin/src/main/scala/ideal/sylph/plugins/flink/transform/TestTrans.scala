package ideal.sylph.plugins.flink.transform

import ideal.sylph.etl.Row
import ideal.sylph.etl.api.RealTimeTransForm


class TestTrans extends RealTimeTransForm {
  /**
    * partition级别的初始化
    **/
  override def open(partitionId: Long, version: Long): Boolean = true

  /**
    * line 级别的 需要注意线程安全问题
    **/
  override def process(value: Row): Array[Row] = {
    Array(value)
  }

  /**
    * partition级别的资源释放
    **/
  override def close(errorOrNull: Throwable): Unit = {}

  override def driverInit(optionMap: java.util.Map[String, Object]): Unit = {
  }

  /**
    * driver 上运行
    */
  override def getRowSchema: Row.Schema = null
}
