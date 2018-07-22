package ideal.sylph.plugins.flink.source

import java.util.Properties

import ideal.sylph.api.etl.Source
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TypeExtractor}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.types.Row

@SerialVersionUID(2L)//使用注解来制定序列化id
class KafkaSource extends Source[StreamTableEnvironment, DataStream[Row]] {

  @transient private var optionMap: java.util.Map[String, Object] = _
  @transient private var tableEnv: StreamTableEnvironment = _

  /**
    * 初始化(driver阶段执行)
    **/
  override def driverInit(
                           tableEnv: StreamTableEnvironment,
                           optionMap: java.util.Map[String, Object]
                         ): Unit = {
    this.optionMap = optionMap
    this.tableEnv = tableEnv
  }

  @transient private lazy val loadStream: DataStream[Row] = {
    val topics = optionMap.get("kafka_topic").asInstanceOf[String]
    val brokers = optionMap.get("kafka_broker") //需要把集群的host 配置到程序所在机器
    val groupid = optionMap.get("kafka_group_id") //消费者的名字
    val offset = optionMap.getOrDefault("auto.offset.reset", "latest") //latest earliest

    val properties = new Properties()
    properties.put("bootstrap.servers", brokers)
    //"enable.auto.commit" -> (false: java.lang.Boolean), //不自动提交偏移量
    //      "session.timeout.ms" -> "30000", //session默认是30秒 超过5秒不提交offect就会报错
    //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
    properties.put("group.id", groupid) //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
    properties.put("auto.offset.reset", offset) //latest   earliest


    val topicSets = java.util.Arrays.asList[String](topics.split(","): _*)
    //org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
    val stream: DataStream[Row] = FlinkEnvUtil.getFlinkEnv(tableEnv).addSource(new FlinkKafkaConsumer010[Row](
      topicSets,
      new RowDeserializer(),
      properties)
    )
    //------------registerDataStream--------------
    val tableName = optionMap.getOrDefault("table_name", null).asInstanceOf[String]
    if (tableName != null) {
      tableEnv.registerDataStream(tableName, stream, "topic,message,key,proctime.proctime")
    }
    stream
  }

  private class RowDeserializer extends KeyedDeserializationSchema[Row] {

    override def isEndOfStream(nextElement: Row): Boolean = {
      false
    }

    override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): Row = {
      Row.of(
        topic, //topic
        new String(message, "utf-8"), //value
        if (messageKey == null) "" else new String(messageKey, "utf-8") //key
      )
    }

    override def getProducedType: TypeInformation[Row] = {
      val types: Array[TypeInformation[_]] = Array(
        TypeExtractor.createTypeInfo(classOf[String]),
        TypeExtractor.createTypeInfo(classOf[String]),
        TypeExtractor.createTypeInfo(classOf[String]) //createTypeInformation[String]
      )
      val rowTypeInfo = new RowTypeInfo(types, Array("topic", "value", "key"))
      //createTypeInformation[Row]
      rowTypeInfo
    }

  }

  //case class People(user_id: String, num: Int, server_time: Timestamp)

  override def getSource: DataStream[Row] = loadStream
}
