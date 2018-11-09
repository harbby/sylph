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
package ideal.sylph.plugins.flink.transform

import ideal.sylph.annotation.Description
import ideal.sylph.etl.api.TransForm
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row

@Description("use test")
@SerialVersionUID(2L) //使用注解来制定序列化id
class TestSqlWindow extends TransForm[DataStream[Row]] {

  override def transform(stream: DataStream[Row]): DataStream[Row] = {
    val execEnv: StreamExecutionEnvironment = stream.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(execEnv)
    val result2: Table = tableEnv.sqlQuery(
      s"""SELECT TUMBLE_START(rowtime, INTERVAL '5' SECOND) AS s,
         |  TUMBLE_END(rowtime, INTERVAL '5' SECOND) AS e,
         |  user_id,
         |  COUNT(1) as cnt
         | FROM tp
         | GROUP BY user_id, TUMBLE(rowtime, INTERVAL '5' SECOND)
         |
      """.stripMargin)

    tableEnv.toAppendStream(result2, classOf[Row])
  }
}
