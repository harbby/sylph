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

import java.nio.charset.StandardCharsets.UTF_8

import ideal.sylph.annotation.{Description, Name, Version}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Created by ideal on 17-4-25.
  * kafka load
  */
@Name("kafka")
@Version("1.0.0")
@Description("this spark kafka 0.10+ source inputStream")
@SerialVersionUID(1L)
@Deprecated
class MyKafkaSource(@transient private val ssc: StreamingContext, private val config: KafkaSourceConfig) {
  /**
    * load stream
    **/
  private lazy val kafkaStream: DStream[Row] = {
    val topics = config.getTopics
    val brokers = config.getBrokers //需要把集群的host 配置到程序所在机器
    val groupid = config.getGroupid //消费者的名字
    val offsetMode = config.getOffsetMode

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[ByteArrayDeserializer], //StringDeserializer
      "value.deserializer" -> classOf[ByteArrayDeserializer], //StringDeserializer
      "enable.auto.commit" -> (false: java.lang.Boolean), //不自动提交偏移量
      //      "session.timeout.ms" -> "30000", //session默认是30秒
      //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
      "group.id" -> groupid, //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
      "auto.offset.reset" -> offsetMode //latest   earliest
    )

    val schema: StructType = StructType(Array(
      StructField("_topic", StringType, nullable = false),
      StructField("_key", StringType, true),
      StructField("_message", StringType, true),
      StructField("_partition", IntegerType, false),
      StructField("_offset", LongType, false),
      StructField("_timestamp", LongType, true),
      StructField("_timestampType", IntegerType, true)
    ))

    val topicSets = topics.split(",")
    val inputStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](topicSets, kafkaParams))

    inputStream.map(record =>
      new GenericRowWithSchema(Array(record.topic(), new String(record.key(), UTF_8), new String(record.value(), UTF_8),
        record.partition(), record.offset(), record.timestamp(), record.timestampType().id), schema).asInstanceOf[Row]
    ) //.window(Duration(10 * 1000))
  }

  //  override def getSource: DStream[Row] = kafkaStream
}