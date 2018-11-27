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

import java.util
import java.util.function.UnaryOperator

import ideal.common.ioc.{Bean, IocFactory}
import ideal.sylph.etl.PipelinePlugin
import ideal.sylph.etl.api.{RealTimeSink, RealTimeTransForm, Sink, TransForm}
import ideal.sylph.runner.spark.etl.{SparkRow, SparkUtil}
import ideal.sylph.spi.NodeLoader
import ideal.sylph.spi.model.PipelinePluginManager
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created by ideal on 17-5-8.
  */
class StructuredNodeLoader(private val pluginManager: PipelinePluginManager, private val bean: Bean) extends NodeLoader[DataFrame] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val iocFactory = IocFactory.create(bean)

  override def loadSource(driverStr: String, config: util.Map[String, Object]): UnaryOperator[DataFrame] = {
    val spark: SparkSession = iocFactory.getInstance(classOf[SparkSession])

    import collection.JavaConverters._
    val source: DataFrame = driverStr match {
      case "kafka" => KafkaSourceUtil.getSource(spark, config)
      case _ => spark.readStream
        .format(driverStr)
        .options(config.asScala.map(x => (x._1, x._2.toString)))
        .load()
    }

    new UnaryOperator[DataFrame] {
      override def apply(stream: DataFrame): DataFrame = {
        logger.info("source {} schema:", driverStr)
        source.printSchema()
        source
      }
    }
  }

  override def loadSink(driverStr: String, config: util.Map[String, Object]): UnaryOperator[DataFrame] = {
    new UnaryOperator[DataFrame] {
      override def apply(stream: DataFrame): DataFrame = {
        //-------启动job-------
        val streamingQuery = loadSinkWithComplic(driverStr, config).apply(stream).start() //start job
        //streamingQuery.stop()
        null
      }
    }
  }

  def loadSinkWithComplic(driverStr: String, config: util.Map[String, Object]): DataFrame => DataStreamWriter[Row] = {
    val driverClass = pluginManager.loadPluginDriver(driverStr, PipelinePlugin.PipelineType.sink)
    val driver: Any = getPluginInstance(driverClass, config)
    val sink: Sink[DataStreamWriter[Row]] = driver match {
      case realTimeSink: RealTimeSink => loadRealTimeSink(realTimeSink)
      case sink: Sink[_] => sink.asInstanceOf[Sink[DataStreamWriter[Row]]]
      case _ => throw new RuntimeException("未知的sink插件:" + driver)
    }

    logger.info("初始化{} 完成", driver)

    stream: DataFrame => {
      //-------启动job-------
      val writer = stream.writeStream
      if (config.containsKey("outputMode")) { //设置输出模式
        writer.outputMode(config.get("outputMode").asInstanceOf[String])
      }
      val jobName = config.get("name").asInstanceOf[String]
      writer.queryName(jobName).trigger(Trigger.ProcessingTime("1 seconds")) //设置触发器

      if (config.containsKey("checkpoint")) {
        writer.option("checkpointLocation", config.get("checkpoint").asInstanceOf[String])
      }
      sink.run(writer)
      writer
    }
  }

  /**
    * transform api 尝试中
    **/
  override def loadTransform(driverStr: String, config: util.Map[String, Object]): UnaryOperator[DataFrame] = {
    val driverClass = pluginManager.loadPluginDriver(driverStr, PipelinePlugin.PipelineType.transform)
    val driver: Any = getPluginInstance(driverClass, config)

    val transform: TransForm[DataFrame] = driver match {
      case realTimeTransForm: RealTimeTransForm => loadRealTimeTransForm(realTimeTransForm)
      case transform: TransForm[_] => transform.asInstanceOf[TransForm[DataFrame]]
      case _ => throw new RuntimeException("未知的TransForm插件:" + driver)
    }
    new UnaryOperator[DataFrame] {
      override def apply(stream: DataFrame): DataFrame = {
        var transStream = transform.transform(stream)
        logger.info("{} schema to :", driver)
        transStream.printSchema()
        transStream
      }
    }
  }

  private[structured] def loadRealTimeSink(realTimeSink: RealTimeSink) = new Sink[DataStreamWriter[Row]]() {
    override def run(stream: DataStreamWriter[Row]): Unit = {
      stream.foreach(new ForeachWriter[Row]() {
        override def process(value: Row): Unit = realTimeSink.process(SparkRow.make(value))

        override def close(errorOrNull: Throwable): Unit = realTimeSink.close(errorOrNull)

        override def open(partitionId: Long, version: Long): Boolean = realTimeSink.open(partitionId, version)
      })
    }
  }

  private[structured] def loadRealTimeTransForm(realTimeTransForm: RealTimeTransForm) = new TransForm[Dataset[Row]]() {
    override def transform(stream: Dataset[Row]): Dataset[Row] = {
      //spark2.x 要对dataSet 进行map操作必须要加上下面一句类型映射
      //implicit val matchError:org.apache.spark.sql.Encoder[Row] = org.apache.spark.sql.Encoders.kryo[Row]
      lazy val rddSchema: StructType = StructType(Array(
        StructField("table", StringType, nullable = true),
        StructField("time", LongType, true),
        StructField("schema", StringType, true),
        StructField("value", MapType.apply(StringType, ObjectType.apply(classOf[Object])), true)
      ))
      //      import collection.JavaConverters._
      //      val mapRowSchema = realTimeTransForm.getRowSchema.getFields.asScala.map(filed => {
      //        StructField(filed.getName, SparkRow.SparkRowParser.parserType(filed.getJavaType), true)
      //      })
      //      RowEncoder.apply(StructType(mapRowSchema))

      implicit val matchError: org.apache.spark.sql.Encoder[Row] = org.apache.spark.sql.Encoders.kryo[Row]
      //implicit val mapenc = RowEncoder.apply(rddSchema)  //此处无法注册 原因是必须是sql基本类型   //Encoders.STRING
      val transStream = stream.mapPartitions(partition => SparkUtil.transFunction(partition, realTimeTransForm))
      //或者使用 transStream.as()
      //transStream.repartition(10)
      transStream
    }
  }

  override def getIocFactory: IocFactory = iocFactory
}
