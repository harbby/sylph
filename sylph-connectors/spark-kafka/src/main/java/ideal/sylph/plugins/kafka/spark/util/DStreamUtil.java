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
package ideal.sylph.plugins.kafka.spark.util;

import ideal.sylph.etl.api.Sink;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * spark 老流 kafka优化
 */
public class DStreamUtil
{
    private static final Logger logger = LoggerFactory.getLogger(DStreamUtil.class);

    private DStreamUtil() {}

    public static DStream<?> getFristDStream(DStream<?> stream)
    {
        if (stream.dependencies().isEmpty()) {
            return stream;
        }
        else {
            return getFristDStream(stream.dependencies().head());
        }
    }

    public static RDD<?> getFristRdd(RDD<?> rdd)
    {
        if (rdd.dependencies().isEmpty()) {
            return rdd;
        }
        else {
            return getFristRdd(rdd.dependencies().head().rdd());
        }
    }

    public static void dstreamAction(JavaDStream<Row> stream, Sink<JavaRDD<Row>> sink)
    {
        DStream<?> fristDStream = getFristDStream(stream.dstream());
        logger.info("数据源驱动:{}", fristDStream.getClass().getName());

        if ("DirectKafkaInputDStream".equals(fristDStream.getClass().getSimpleName())) {
            logger.info("发现job 数据源是kafka,将开启空job优化 且 自动上报offect");
            stream.foreachRDD(rdd -> {
                RDD<?> kafkaRdd = getFristRdd(rdd.rdd()); //rdd.dependencies(0).rdd
                OffsetRange[] offsetRanges = ((HasOffsetRanges) kafkaRdd).offsetRanges();
                if (kafkaRdd.count() > 0) {
                    sink.run(rdd); //执行业务操作
                }
                ((CanCommitOffsets) fristDStream).commitAsync(offsetRanges);
            });
        }
        else { //非kafka数据源 暂时无法做任何优化
            stream.foreachRDD(sink::run);
        }
    }
}
