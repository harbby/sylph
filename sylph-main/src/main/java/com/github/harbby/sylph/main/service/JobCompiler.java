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
package com.github.harbby.sylph.main.service;

import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.github.harbby.sylph.parser.AntlrSqlParser;
import com.github.harbby.sylph.spi.CompileTask;
import com.github.harbby.sylph.spi.OperatorInfo;
import com.github.harbby.sylph.spi.Runner;
import com.github.harbby.sylph.spi.dao.Job;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobDag;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.job.JobParser;
import org.fusesource.jansi.Ansi;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

public class JobCompiler
{
    private final JobEngineManager jobEngineManager;
    private final OperatorManager operatorManager;

    @Autowired
    public JobCompiler(JobEngineManager jobEngineManager, OperatorManager operatorManager)
    {
        this.jobEngineManager = jobEngineManager;
        this.operatorManager = operatorManager;
    }

    public JobDag<?> compileJob(Job job)
            throws Exception
    {
        JobEngineWrapper engineWrapper = jobEngineManager.getJobEngine(job.getType());
        String jobName = job.getJobName();

        JobConfig jobConfig = engineWrapper.runner().analyzeConfig(job.getConfig());
        Map<String, OperatorInfo> engineOperators = operatorManager.getOperators(engineWrapper.getJobEngine());

        //---add engines jars
        List<URL> dependJars = new ArrayList<>();
        //add plugin operator jars
        JobParser jobParser = engineWrapper.getJobEngine().analyze(job.getQueryText());
        for (JobParser.DependOperator dep : jobParser.getDependOperators()) {
            OperatorInfo op = engineOperators.get(dep.getConnector() + "\u0001" + dep.getType());
            if (op == null) {
                throw new IllegalStateException("not found operator " + dep.getConnector());
            }
            dep.setClassName(op.getDriverClass());
            for (File file : op.moduleFiles()) {
                dependJars.add(file.toURI().toURL());
            }
        }

        return doCompile(engineWrapper, dependJars, jobParser, jobName, jobConfig);
    }

    private static JobDag<?> doCompile(JobEngineWrapper engineWrapper,
            List<URL> dependJars,
            JobParser jobParser,
            String jobName,
            JobConfig jobConfig)
    {
        JobEngine jobEngine = engineWrapper.getJobEngine();
        Runner runner = engineWrapper.runner();
        URLClassLoader jobClassLoader = new URLClassLoader(dependJars.toArray(new URL[0]),
                runner.getClass().getClassLoader());

        CompileTask compileTask = new CompileTask(jobParser, jobEngine, jobConfig);
        JVMLauncher<Serializable> launcher = JVMLaunchers.<Serializable>newJvm()
                .task(compileTask)
                .setConsole((line) -> System.out.println(new Ansi().fg(YELLOW).a("[" + jobName + "] ").fg(GREEN).a(line).reset().toString()))
                .addUserJars(new URL[] {
                        AntlrSqlParser.class.getProtectionDomain().getCodeSource().getLocation(), //parser
                        JVMLauncher.class.getProtectionDomain().getCodeSource().getLocation(), //gadtry
                        JobEngine.class.getProtectionDomain().getCodeSource().getLocation(), //spi
                })
                .addUserJars(runner.getClassloaderJars())
                .addUserJars(dependJars)
                .addVmOps("--add-opens=java.base/java.lang=ALL-UNNAMED")//flink
                .addVmOps("--add-opens=java.base/java.util=ALL-UNNAMED")
                //spark
                .addVmOps("--add-opens=java.base/java.nio=ALL-UNNAMED")
                .addVmOps("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .addVmOps("--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED")
                .setClassLoader(jobClassLoader)
                .filterThisJVMClass()
                .build();
        Serializable graph = launcher.startAndGet();
        JobDag<?> jobDag = new JobDag<>(graph, jobName);
        dependJars.stream().filter(x -> !x.getPath().contains("hadoop-lib")).forEach(jobDag::addJar);
        return jobDag;
    }
}
