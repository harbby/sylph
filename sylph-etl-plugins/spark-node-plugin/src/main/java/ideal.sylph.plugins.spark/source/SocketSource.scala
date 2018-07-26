package ideal.sylph.plugins.spark.source

import ideal.sylph.api.etl.Source
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by ideal on 17-4-25.
  */
class SocketSource extends Source[StreamingContext, DStream[Row]] {

  private var ssc: StreamingContext = _
  private var props: java.util.Map[String, Object] = _

  /**
    * 初始化(driver阶段执行)
    **/
  override def driverInit(ssc: StreamingContext, props: java.util.Map[String, Object]): Unit = {
    this.ssc = ssc
    this.props = props
  }

  private lazy val loadStream: DStream[Row] = {
    val socketLoad = props.get("socketLoad").asInstanceOf[String]

    val schema: StructType = StructType(Array(
      StructField("host", StringType, nullable = true),
      StructField("port", StringType, true),
      StructField("value", StringType, true)
    ))

    socketLoad.split(",").filter(_.contains(":")).toSet[String].map(socket => {
      val Array(host, port) = socket.split(":")
      val socketSteam: DStream[Row] = ssc.socketTextStream(host, port.toInt)
        .map(value => new GenericRowWithSchema(Array(host, port.toInt, value), schema = schema))
      socketSteam
    }).reduce((x, y) => x.union(y))
  }

  def addSink(sink: Sink[JavaRDD[Row]], transForms: List[TransForm[DStream[Row]]]): Unit = {

    var transStream = loadStream
    transForms.foreach(transForm => {
      transStream = transForm.transform(transStream)
    })

    transStream.foreachRDD(rdd => sink.run(rdd))
  }

  override def getSource: DStream[Row] = loadStream
}
