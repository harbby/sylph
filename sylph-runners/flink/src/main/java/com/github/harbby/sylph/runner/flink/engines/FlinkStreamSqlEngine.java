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
package com.github.harbby.sylph.runner.flink.engines;

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.runner.flink.FlinkJobConfig;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.job.JobParser;
import com.github.harbby.sylph.spi.job.SqlJobParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

@Name("FlinkStreamSql")
@Description("this is flink stream sql etl Actuator")
public class FlinkStreamSqlEngine
        implements JobEngine
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamSqlEngine.class);

    @Override
    public JobParser analyze(String flowBytes)
    {
        return SqlJobParser.parser(flowBytes);
    }

    @Override
    public List<Class<?>> keywords()
    {
        return ImmutableList.of(
                org.apache.flink.streaming.api.datastream.DataStream.class,
                org.apache.flink.types.Row.class);
    }

    @Override
    public Serializable compileJob(JobParser inJobParser, JobConfig jobConfig)
            throws Exception
    {
        SqlJobParser sqlJobParser = (SqlJobParser) inJobParser;
        System.out.println("************ job start ***************");
        StreamExecutionEnvironment execEnv = FlinkEnvFactory.createFlinkEnv((FlinkJobConfig) jobConfig);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnv);
        StreamSqlBuilder streamSqlBuilder = new StreamSqlBuilder(tableEnv);
        for (SqlJobParser.StatementNode statement : sqlJobParser.getTree()) {
            streamSqlBuilder.buildStreamBySql(statement);
        }
        StreamGraph streamGraph = execEnv.getStreamGraph();
        return streamGraph.getJobGraph();
    }
}
