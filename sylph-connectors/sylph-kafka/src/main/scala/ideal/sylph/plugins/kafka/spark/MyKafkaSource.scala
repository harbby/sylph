/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.plugins.kafka.spark

import ideal.sylph.annotation.{Description, Name, Version}
import ideal.sylph.etl.PluginConfig
import ideal.sylph.etl.api.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Created by ideal on 17-4-25.
  * kafka load
  */
@Name("kafka")
@Version("1.0.0")
@Description("this spark kafka source inputStream")
@SerialVersionUID(1L)
class MyKafkaSource(@transient private val ssc: StreamingContext, private val config: KafkaSourceConfig) extends Source[DStream[Row]] {

  /**
    * load stream
    **/
  private lazy val kafkaStream: InputDStream[ConsumerRecord[String, String]] = {
    val topics = config.topics
    val brokers = config.brokers //需要把集群的host 配置到程序所在机器
    val groupid = config.groupid //消费者的名字
    val offset = config.offsetMode //

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean), //不自动提交偏移量
      //      "session.timeout.ms" -> "30000", //session默认是30秒 超过5秒不提交offect就会报错
      //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
      "group.id" -> groupid, //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
      "auto.offset.reset" -> offset //latest   earliest
    )

    val topicSets = topics.split(",")
    KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topicSets, kafkaParams))
  }

  override def getSource: DStream[Row] = {
    val schema: StructType = StructType(Array(
      StructField("topic", StringType, nullable = true),
      StructField("value", StringType, true),
      StructField("key", StringType, true)
    ))

    kafkaStream.map(record =>
      new GenericRowWithSchema(Array(record.topic(), record.value(), record.key()), schema)
    ).asInstanceOf[DStream[Row]] //.window(Duration(10 * 1000))
  }
}

@SerialVersionUID(2L)
private[this] class KafkaSourceConfig extends PluginConfig {
  @Name("kafka_topic")
  @Description("this is kafka topic list")
  var topics: String = "test1,test2"

  @Name("kafka_broker")
  @Description("this is kafka broker list")
  var brokers: String = "localhost:9092"

  @Name("kafka_group_id")
  @Description("this is kafka_group_id")
  var groupid: String = "streamEtl1"

  @Name("auto.offset.reset")
  @Description("this is auto.offset.reset mode")
  var offsetMode = "latest"
}