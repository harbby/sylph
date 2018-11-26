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

import java.util.Objects

import ideal.sylph.annotation.{Description, Name, Version}
import ideal.sylph.etl.PluginConfig
import ideal.sylph.etl.api.{Sink, Source, TransForm}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by ideal on 17-4-25.
  */
@Name("socket")
@Version("1.0.0")
@Description("this spark socket source inputStream")
@SerialVersionUID(1L)
class SocketSource(@transient private val ssc: StreamingContext, private val config: SocketSourceConfig) extends Source[DStream[Row]] {

  private lazy val loadStream: DStream[Row] = {
    val socketLoad = Objects.requireNonNull(config.hosts, "socketLoad is not setting")

    val schema: StructType = StructType(Array(
      StructField("host", StringType, nullable = true),
      StructField("port", StringType, true),
      StructField("value", StringType, true)
    ))

    socketLoad.split(",").filter(_.contains(":")).toSet[String].map(socket => {
      val Array(host, port) = socket.split(":")
      val socketSteam: DStream[Row] = ssc.socketTextStream(host, port.toInt)
        .map(value => new GenericRowWithSchema(Array(host, port.toInt, value), schema = schema))
      socketSteam
    }).reduce((x, y) => x.union(y))
  }

  def addSink(sink: Sink[JavaRDD[Row]], transForms: List[TransForm[DStream[Row]]]): Unit = {

    var transStream = loadStream
    transForms.foreach(transForm => {
      transStream = transForm.transform(transStream)
    })

    transStream.foreachRDD(rdd => sink.run(rdd))
  }

  override def getSource: DStream[Row] = loadStream
}

@SerialVersionUID(2L)
private[this] class SocketSourceConfig extends PluginConfig {
  @Name("socket_hosts")
  @Description("this is socket_hosts list")
  var hosts: String = "localhost:9999"
}