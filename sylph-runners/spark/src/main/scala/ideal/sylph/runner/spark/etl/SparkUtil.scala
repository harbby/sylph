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
package ideal.sylph.runner.spark.etl

import ideal.sylph.etl.api.RealTimeTransForm
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row

object SparkUtil {
  val transFunction = (partition: Iterator[Row], realTimeTransForm: RealTimeTransForm) => {
    var errorOrNull: Exception = null
    val schema = realTimeTransForm.getRowSchema
    try {
      val partitionId = TaskContext.getPartitionId()
      if (realTimeTransForm.open(partitionId, 0))
        partition.flatMap(row => {
          //TODO: SparkRow.parserRow(x) with schema ?
          realTimeTransForm.process(SparkRow.make(row)).map(x => SparkRow.parserRow(x))
        })
      else Iterator.empty
    }
    catch {
      case e: Exception => errorOrNull = e
        Iterator.empty //转换失败 这批数据都丢弃
    } finally {
      realTimeTransForm.close(errorOrNull) //destroy()
    }
  }
}
