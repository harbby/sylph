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

import java.util
import java.util.Date
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
@Name("test")
@Description("this flink test source inputStream")
@Version("1.0.0")
@SerialVersionUID(2L) //使用注解来制定序列化id
class TestSource extends Source[StreamTableEnvironment, DataStream[Row]] {

  @transient private var optionMap: java.util.Map[String, Object] = _
  @transient private var tableEnv: StreamTableEnvironment = _

  @transient private lazy val loadStream: DataStream[Row] = {
    val stream = FlinkEnvUtil.getFlinkEnv(tableEnv).addSource(new MyDataSource)
    val tableName = optionMap.getOrDefault("table_name", null).asInstanceOf[String]
    if (tableName != null) {
      tableEnv.registerDataStream(tableName, stream)
    }
    stream
  }

  /**
    * 初始化(driver阶段执行)
    **/
  override def driverInit(tableEnv: StreamTableEnvironment,
                          optionMap: util.Map[String, Object]): Unit = {
    this.optionMap = optionMap
    this.tableEnv = tableEnv
  }

  override def getSource(): DataStream[Row] = loadStream

  private class MyDataSource
    extends RichParallelSourceFunction[Row] with ResultTypeQueryable[Row] {
    private var running = true

    @throws[Exception]
    override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit = {
      val startTime = System.currentTimeMillis
      val numElements = 20000000
      val numKeys = 10
      var value = 1L
      var count = 0L
      while (running) {
        val user_id = "uid:" + value
        val msg = new JSONObject(Map[String, String]("user_id" -> user_id, "ip" -> "127.0.0.1")).toString()
        val serverTime: java.lang.Long = new Date().getTime()
        val row = Row.of("key" + value, msg, serverTime)
        sourceContext.collect(row)
        count += 1
        value += 1
        if (value > numKeys) value = 1L
        TimeUnit.MILLISECONDS.sleep(100)
      }
      val endTime = System.currentTimeMillis
    }

    override def getProducedType: TypeInformation[Row] = {
      val types: Array[TypeInformation[_]] = Array(
        TypeExtractor.createTypeInfo(classOf[String]),
        TypeExtractor.createTypeInfo(classOf[String]),
        TypeExtractor.createTypeInfo(classOf[Long]) //createTypeInformation[String]
      )
      val rowTypeInfo = new RowTypeInfo(types, Array("key", "value", "server_time"))
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
