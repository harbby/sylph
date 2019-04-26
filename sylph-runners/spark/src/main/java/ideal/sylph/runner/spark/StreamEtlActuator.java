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
package ideal.sylph.runner.spark;

import com.github.harbby.gadtry.collection.mutable.MutableList;
import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.job.EtlJobActuatorHandle;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import javax.validation.constraints.NotNull;

import java.net.URLClassLoader;
import java.util.List;

@Name("Spark_StreamETL")
@Description("spark1.x spark streaming StreamETL")
@JobActuator.Mode(JobActuator.ModeType.STREAM_ETL)
public class StreamEtlActuator
        extends EtlJobActuatorHandle
{
    private final PipelinePluginManager pluginManager;

    @Autowired
    public StreamEtlActuator(RunnerContext runnerContext)
    {
        List<Class<?>> filterClass = MutableList.of(
                org.apache.spark.streaming.StreamingContext.class,
                org.apache.spark.streaming.dstream.DStream.class,
                org.apache.spark.sql.Row.class
        );
        this.pluginManager = SparkRunner.createPipelinePluginManager(runnerContext, filterClass);
    }

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        return JobHelper.build1xJob(jobId, (EtlFlow) flow, jobClassLoader, pluginManager);
    }

    @Override
    public PipelinePluginManager getPluginManager()
    {
        return pluginManager;
    }
}
