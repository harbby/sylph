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
package ideal.sylph.runner.spark.etl.structured

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object KafkaSourceUtil {
  private val logger: Logger = LoggerFactory.getLogger(KafkaSourceUtil.getClass)

  /**
    * 下面这些参数 是结构化流官网 写明不支持的参数
    **/
  val filterList = List[String](
    "kafka_group_id", "group.id",
    "key.deserializer",
    "value.deserializer",
    "key.serializer",
    "value.serializer",
    "enable.auto.commit",
    "interceptor.classes"
  )


  /**
    * 对配置进行解析变换
    **/
  private def configParser(optionMap: java.util.Map[String, AnyRef]): mutable.Map[String, String] = {
    import collection.JavaConverters._
    optionMap.asScala.filter(x => {
      if (filterList.contains(x._1)) {
        logger.warn("spark结构化流引擎 忽略参数:key[{}] value[{}]", Array(x._1, x._2): _*)
        false
      } else {
        true
      }
    }).map(x => {
      val key = x._1 match {
        case "kafka_topic" => "subscribe"
        case "kafka_broker" => "kafka.bootstrap.servers"
        case "auto.offset.reset" => "startingOffsets" //注意结构化流上面这里有两个参数
        case _ => x._1
      }
      (key, x._2.toString)
    })
  }

  def getSource(spark: SparkSession, optionMap: java.util.Map[String, AnyRef]): Dataset[Row] = {
    val df = spark.readStream
      .format("kafka")
      .options(configParser(optionMap))
      .load()

    val columns = df.columns.map {
      case "key" => "CAST(key AS STRING) as key"
      case "value" => "CAST(value AS STRING) as value"
      case that => that
    }
    df.selectExpr(columns: _*) //对输入的数据进行 cast转换
  }
}
