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

import ideal.sylph.etl.Row.DefaultRow
import org.apache.spark.sql.Row

object SparkRow {
  def make(row: Row): SparkRow = new SparkRow(row)

  def parserRow(row: ideal.sylph.etl.Row): Row = row match {
    case row1: SparkRow => row1.get()
    case row1: DefaultRow => Row.apply(row1.getValues)
    case _ =>
      throw new RuntimeException(" not souch row type: " + row.getClass)
  }
}

class SparkRow(private val row: Row) extends ideal.sylph.etl.Row {

  def get() = row

  @Override
  override def mkString(seq: String): String = row.mkString(seq)

  override def getAs[T](key: String): T = row.getAs(key).asInstanceOf[T]

  override def getAs[T](i: Int): T = row.getAs(i).asInstanceOf[T]

  override def size(): Int = row.size

  override def toString(): String = row.toString()
}
