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
package ideal.sylph.runner.spark

import java.util
import java.util.function.Consumer

import ideal.sylph.runner.spark.etl.sparkstreaming.DStreamUtil.{getFristDStream, getFristRdd}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

object SqlUtil {
  private val logger = LoggerFactory.getLogger(classOf[SparkStreamingSqlActuator])

  def registerStreamTable(inputStream: DStream[Row],
                          tableName: String,
                          schema: StructType,
                          handlers: util.List[Consumer[SparkSession]]): Unit = {

    import collection.JavaConverters._
    val its = handlers.asScala

    val spark = SparkSession.builder.config(inputStream.context.sparkContext.getConf).getOrCreate()
    inputStream.foreachRDD(rdd => {
      //import spark.implicits._
      val df = spark.createDataFrame(rdd, schema)
      df.createOrReplaceTempView(tableName)
      //df.show()

      val firstDStream = getFristDStream(inputStream)
      if ("DirectKafkaInputDStream".equals(firstDStream.getClass.getSimpleName)) {
        val kafkaRdd = getFristRdd(rdd) //rdd.dependencies(0).rdd
        if (kafkaRdd.count() > 0) {
          its.foreach(_.accept(spark)) //执行业务操作
        }

        //val offsetRanges = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //firstDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } else {
        its.foreach(_.accept(spark))
      }
    })
  }

}
