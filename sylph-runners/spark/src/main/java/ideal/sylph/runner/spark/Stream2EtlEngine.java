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

import com.github.harbby.gadtry.collection.mutable.MutableSet;
import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.job.EtlJobEngineHandle;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;

import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.net.URLClassLoader;
import java.util.Set;

@Name("Spark_Structured_StreamETL")
@Description("spark2.x Structured streaming StreamETL")
public class Stream2EtlEngine
        extends EtlJobEngineHandle
{
    private final RunnerContext runnerContext;

    @Autowired
    public Stream2EtlEngine(RunnerContext runnerContext)
    {
        this.runnerContext = runnerContext;
    }

    @NotNull
    @Override
    public Serializable formJob(String jobId, Flow inFlow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        return JobHelper.build2xJob(jobId, (EtlFlow) inFlow, jobClassLoader, getConnectorStore());
    }

    @Override
    public ConnectorStore getConnectorStore()
    {
        Set<Class<?>> filterClass = MutableSet.of(
                org.apache.spark.sql.SparkSession.class,
                org.apache.spark.sql.Dataset.class,
                org.apache.spark.sql.Row.class
        );
        return runnerContext.createConnectorStore(filterClass, SparkRunner.class);
    }
}
