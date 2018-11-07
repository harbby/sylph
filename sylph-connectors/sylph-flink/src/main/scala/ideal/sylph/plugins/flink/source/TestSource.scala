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
package ideal.sylph.plugins.flink.source

import java.util.Random
import java.util.concurrent.TimeUnit

import ideal.sylph.annotation.{Description, Name, Version}
import ideal.sylph.etl.api.Source
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo, TypeExtractor}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.util.parsing.json.JSONObject

/**
  * test source
  **/
@Name("flink_test_source")
@Description("this flink test source inputStream")
@Version("1.0.0")
@SerialVersionUID(2L) //使用注解来制定序列化id
class TestSource(@transient private val tableEnv: StreamTableEnvironment) extends Source[DataStream[Row]] {

  @transient private lazy val loadStream: DataStream[Row] = {
    FlinkEnvUtil.getFlinkEnv(tableEnv).addSource(new MyDataSource)
  }


  override def getSource(): DataStream[Row] = loadStream

  private class MyDataSource
    extends RichParallelSourceFunction[Row] with ResultTypeQueryable[Row] {
    private var running = true

    @throws[Exception]
    override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit = {
      val random = new Random
      val numKeys = 10
      var count = 1L
      while (running) {
        val eventTime: java.lang.Long = System.currentTimeMillis - random.nextInt(10 * 1000) //表示数据已经产生了 但是会有10秒以内的延迟
        val user_id = "uid_" + count
        val msg = JSONObject(Map[String, String]("user_id" -> user_id, "ip" -> "127.0.0.1")).toString()
        val row = Row.of("key" + random.nextInt(10), msg, eventTime)
        sourceContext.collect(row)
        count += 1
        if (count > numKeys) count = 1L
        TimeUnit.MILLISECONDS.sleep(100)
      }
    }

    override def getProducedType: TypeInformation[Row] = {
      val types: Array[TypeInformation[_]] = Array(
        TypeExtractor.createTypeInfo(classOf[String]),
        TypeExtractor.createTypeInfo(classOf[String]),
        TypeExtractor.createTypeInfo(classOf[Long]) //createTypeInformation[String]
      )
      val rowTypeInfo = new RowTypeInfo(types, Array("key", "value", "event_time"))
      //createTypeInformation[Row]
      rowTypeInfo
    }

    override def cancel(): Unit = {
      running = false
    }

    override def close(): Unit = {
      this.cancel()
      super.close()
    }
  }

}
