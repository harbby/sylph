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
import com.github.harbby.gadtry.jvm.JVMException;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobEngineHandle;
import org.fusesource.jansi.Ansi;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

/**
 * see: org.apache.spark.deploy.Client#createContainerLaunchContext
 * see: org.apache.spark.deploy.yarn.ApplicationMaster#startUserApplication
 */
@Name("SparkMainClass")
@Description("this is FlinkMainClassActuator Actuator")
public class SparkMainClassEngine
        implements JobEngineHandle
{
    private static final URLClassLoader classLoader = (URLClassLoader) SparkMainClassEngine.class.getClassLoader();

    @Override
    public Serializable formJob(String jobId, Flow flow, JobConfig jobConfig, List<URL> pluginJars)
            throws Exception
    {
        return compile(jobId, (StringFlow) flow, pluginJars);
    }

    private static Serializable compile(String jobId, StringFlow flow, List<URL> pluginJars)
            throws JVMException
    {
        JVMLauncher<String> launcher = JVMLaunchers.<String>newJvm()
                .setConsole((line) -> System.out.print(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .task(() -> {
                    Class<?> mainClass = Class.forName(flow.mainClass);
                    Method main = mainClass.getMethod("main", String[].class);
                    try {
                        main.invoke(null, (Object) new String[0]);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                })
                .setClassLoader(classLoader)
                .addUserjars(ImmutableList.copy(classLoader.getURLs())) //flink jars + runner jar
                .addUserjars(pluginJars)
                .build();

        launcher.startAndGet();
        return null;
    }

    @Override
    public Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return new StringFlow(flowBytes);
    }

    @Override
    public List<Class<?>> keywords()
    {
        return Collections.emptyList();
    }

    public static class StringFlow
            extends Flow
    {
        private final String mainClass;

        public StringFlow(byte[] flowBytes)
        {
            this.mainClass = new String(flowBytes, UTF_8).trim();
        }

        @Override
        public String toString()
        {
            return mainClass;
        }
    }
}
