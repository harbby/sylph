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

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.job.EtlJobEngineHandle;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;

import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.net.URL;
import java.util.List;

@Name("Spark_StreamETL")
@Description("spark1.x spark streaming StreamETL")
public class StreamEtlEngine
        extends EtlJobEngineHandle
{
    private final RunnerContext runnerContext;

    @Autowired
    public StreamEtlEngine(RunnerContext runnerContext)
    {
        super(runnerContext);
        this.runnerContext = runnerContext;
    }

    @NotNull
    @Override
    public Serializable formJob(String jobId, Flow flow, JobConfig jobConfig, List<URL> pluginJars)
            throws Exception
    {
        return JobHelper.build1xJob(jobId, (EtlFlow) flow, pluginJars, runnerContext.getLatestMetaData(this));
    }

    @Override
    public List<Class<?>> keywords()
    {
        return ImmutableList.of(
                org.apache.spark.streaming.dstream.DStream.class,
                org.apache.spark.streaming.api.java.JavaDStream.class,
                org.apache.spark.rdd.RDD.class,
                org.apache.spark.api.java.JavaRDD.class,
                org.apache.spark.sql.Row.class
        );
    }
}
