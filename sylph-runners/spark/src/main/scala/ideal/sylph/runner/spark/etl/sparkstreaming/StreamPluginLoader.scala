package ideal.sylph.runner.spark.etl.sparkstreaming

import java.util.function.UnaryOperator

import ideal.sylph.spi.NodeLoader
import ideal.sylph.api.etl._
import ideal.sylph.runner.spark.etl.{SparkRow, SparkUtil}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by ideal on 17-5-8.
  * spark 1.x spark Streaming
  */
class StreamPluginLoader extends NodeLoader[StreamingContext, DStream[Row]] {

  override def loadSource(spark: StreamingContext, props: java.util.Map[String, Object]): UnaryOperator[DStream[Row]] = {
    val driver = props.get("driver").asInstanceOf[String]
    val source = driver match {
      case _ => val driverClass = Class.forName(driver)
        val newSource = driverClass.newInstance().asInstanceOf[Source[StreamingContext, DStream[Row]]]
        newSource.driverInit(spark, props) //传入参数
        newSource
    }
    new UnaryOperator[DStream[Row]] {
      override def apply(stream: DStream[Row]): DStream[Row] = source.getSource
    }
  }

  override def loadSink(config: java.util.Map[String, Object]): UnaryOperator[DStream[Row]] = {
    val driverStr = Class.forName(config.get("driver").asInstanceOf[String])
    val driver: Any = driverStr.newInstance()

    val sink: Sink[RDD[Row]] = driver match {
      case realTimeSink: RealTimeSink =>
        realTimeSink.driverInit(config) //传入参数
        loadRealTimeSink(realTimeSink)
      case a2: Sink[RDD[Row]] => a2.driverInit(config)
        a2
      case _ => throw new RuntimeException("未知的sink插件:" + driver)
    }

    new UnaryOperator[DStream[Row]] {
      override def apply(stream: DStream[Row]): DStream[Row] = {
        DStreamUtil.DstreamParser(stream, sink) //这里处理偏移量提交问题
        null
      }
    }
  }

  /**
    * transform api 尝试中
    **/
  override def loadTransform(config: java.util.Map[String, Object]): UnaryOperator[DStream[Row]] = {
    val driverStr = Class.forName(config.get("driver").asInstanceOf[String])
    //val driver = Class.forName(driverStr).asSubclass(classOf[TransForm[DStream[Row]]])
    val driver: Any = driverStr.newInstance()

    val transform: TransForm[DStream[Row]] = driver match {
      case realTimeTransForm: RealTimeTransForm =>
        realTimeTransForm.driverInit(config) ////传入这个模块的参数
        loadRealTimeTransForm(realTimeTransForm)
      case a2: TransForm[DStream[Row]] =>
        a2.driverInit(config)
        a2
      case _ => throw new RuntimeException("未知的Transform插件:" + driver)
    }
    new UnaryOperator[DStream[Row]] {
      override def apply(stream: DStream[Row]): DStream[Row] = transform.transform(stream)
    }
  }


  private[sparkstreaming] def loadRealTimeSink(realTimeSink: RealTimeSink) = new Sink[RDD[Row]] {
    override def run(rdd: RDD[Row]): Unit = {
      rdd.foreachPartition(partition => {
        var errorOrNull: Throwable = null
        try {
          val partitionId = TaskContext.getPartitionId()
          val openOK = realTimeSink.open(partitionId, 0) //初始化 返回是否正常 如果正常才处理数据
          if (openOK) partition.foreach(row => {
            try {
              realTimeSink.process(SparkRow.make(row))
            } catch {
              case e: Exception => //忽略出错的这一行
            }
          })

        } catch {
          case e: Exception => errorOrNull = e //open出错了
        } finally {
          realTimeSink.close(errorOrNull) //destroy()
        }
      })
    }
  }

  private[sparkstreaming] def loadRealTimeTransForm(realTimeTransForm: RealTimeTransForm) = new TransForm[DStream[Row]] {
    override def transform(stream: DStream[Row]): DStream[Row] =
      stream.mapPartitions(partition => SparkUtil.transFunction(partition, realTimeTransForm))
  }
}
