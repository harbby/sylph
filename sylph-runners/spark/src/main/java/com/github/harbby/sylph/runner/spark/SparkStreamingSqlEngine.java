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
package com.github.harbby.sylph.runner.spark;

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.job.JobParser;
import com.github.harbby.sylph.spi.job.SqlJobParser;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.github.harbby.sylph.runner.spark.SQLHepler.buildSql;

/**
 * DStreamGraph graph = inputStream.graph();   //spark graph ?
 */
@Name("SparkStreamingSql")
@Description("this is spark streaming sql Actuator")
public class SparkStreamingSqlEngine
        implements JobEngine
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlEngine.class);

    @Override
    public JobParser analyze(String flowBytes)
    {
        return SqlJobParser.parser(flowBytes);
    }

    @Override
    public List<Class<?>> keywords()
    {
        return ImmutableList.of(
                org.apache.spark.streaming.dstream.DStream.class,
                org.apache.spark.streaming.api.java.JavaDStream.class,
                org.apache.spark.rdd.RDD.class,
                org.apache.spark.api.java.JavaRDD.class,
                org.apache.spark.sql.Row.class);
    }

    @Override
    public Serializable compileJob(JobParser inJobParser, JobConfig jobConfig)
            throws Exception
    {
        SqlJobParser flow = (SqlJobParser) inJobParser;
        int batchDuration = 5; //sparkJobConfig.getSparkStreamingBatchDuration();
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        final Supplier<StreamingContext> appGetter = (Supplier<StreamingContext> & Serializable) () -> {
            SparkSession sparkSession = SparkUtil.getSparkSession(isCompile.get());
            StreamingContext ssc = new StreamingContext(sparkSession.sparkContext(), Duration.apply(batchDuration));

            //build sql
            SqlAnalyse analyse = new SparkStreamingSqlAnalyse(ssc, isCompile.get());
            try {
                buildSql(analyse, flow);
            }
            catch (Exception e) {
                throw new IllegalStateException("job compile failed", e);
            }
            return ssc;
        };
        appGetter.get();
        isCompile.set(false);
        return (Serializable) appGetter;
    }
}
