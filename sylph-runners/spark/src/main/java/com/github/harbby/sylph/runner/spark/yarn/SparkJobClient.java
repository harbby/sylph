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
package com.github.harbby.sylph.runner.spark.yarn;

import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.sylph.runner.spark.SparkJobConfig;
import com.github.harbby.sylph.runner.spark.SparkRunner;
import com.github.harbby.sylph.runner.spark.runtime.SparkAppMain;
import com.github.harbby.sylph.spi.JobClient;
import com.github.harbby.sylph.spi.dao.JobRunState;
import com.github.harbby.sylph.spi.job.DeployResponse;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobDag;
import com.github.harbby.sylph.yarn.YarnClientFactory;
import com.github.harbby.sylph.yarn.YarnDeployResponse;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkJobClient
        implements JobClient
{
    private final String sparkHome;

    public SparkJobClient(String sparkHome)
    {
        this.sparkHome = sparkHome;
    }

    @Override
    public DeployResponse deployJobOnYarn(JobDag<?> job, JobConfig jobConfig)
            throws Exception
    {
        try (YarnClient yarnClient = YarnClientFactory.createYarnClient()) {
            ApplicationId applicationId = this.run(yarnClient, job, (SparkJobConfig) jobConfig);
            String webUi = yarnClient.getApplicationReport(applicationId).getTrackingUrl();
            return new YarnDeployResponse(applicationId, webUi);
        }
    }

    @Override
    public void closeJobOnYarn(String runId)
            throws Exception
    {
        try (YarnClient yarnClient = YarnClientFactory.createYarnClient()) {
            yarnClient.killApplication(Apps.toAppID(runId));
        }
    }

    @Override
    public Map<Integer, JobRunState.Status> getAllJobStatus(Map<Integer, String> runIds)
            throws Exception
    {
        try (YarnClient yarnClient = YarnClientFactory.createYarnClient()) {
            GetApplicationsRequest request = GetApplicationsRequest.newInstance();
            request.setApplicationTypes(Collections.singleton(SparkRunner.APPLICATION_TYPE));
            request.setApplicationStates(EnumSet.of(YarnApplicationState.RUNNING, YarnApplicationState.ACCEPTED, YarnApplicationState.SUBMITTED));
            Set<String> yarnApps = yarnClient.getApplications(request).stream()
                    .map(x -> x.getApplicationId().toString()).collect(Collectors.toSet());

            return runIds.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    v -> yarnApps.contains(v.getValue()) ? JobRunState.Status.RUNNING : JobRunState.Status.STOP));
        }
    }

    public ApplicationId run(YarnClient yarnClient, JobDag<?> job, SparkJobConfig jobConfig)
            throws Exception
    {
        System.setProperty("SPARK_YARN_MODE", "true");
        SparkConf sparkConf = createSparkConf(job, jobConfig);
        //set Depends set spark.yarn.dist.jars and spark.yarn.dist.files
        File jobGraphFile = setDistJars(job, sparkConf);

        String[] args = getArgs(jobGraphFile);
        ClientArguments clientArguments = new ClientArguments(args);   // spark-2.0.0+
        Client appClient = new SylphSparkYarnClient(clientArguments, sparkConf, yarnClient, jobConfig.getQueue());
        return appClient.submitApplication();
    }

    private SparkConf createSparkConf(JobDag<?> job, SparkJobConfig jobConfig)
    {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.driver.extraJavaOptions", "-XX:PermSize=64M -XX:MaxPermSize=128M");
        //sparkConf.set("spark.yarn.stagingDir", appHome);
        //-------------
        sparkConf.set("spark.executor.instances", jobConfig.getNumExecutors() + "");   //EXECUTOR_COUNT
        sparkConf.set("spark.executor.memory", jobConfig.getExecutorMemory());  //EXECUTOR_MEMORY
        sparkConf.set("spark.executor.cores", jobConfig.getExecutorCores() + "");

        sparkConf.set("spark.driver.cores", jobConfig.getDriverCores() + "");
        sparkConf.set("spark.driver.memory", jobConfig.getDriverMemory());
        //--------------

        sparkConf.setSparkHome(sparkHome);

        sparkConf.setMaster("yarn");
        sparkConf.setAppName(job.getName());

        sparkConf.set("spark.submit.deployMode", "cluster"); // worked

        return sparkConf;
    }

    private File setDistJars(JobDag<?> job, SparkConf sparkConf)
            throws IOException
    {
        File jobGraphFile = File.createTempFile("sylph_spark", "job.graph");
        byte[] bytes = Serializables.serialize((Serializable) job.getGraph());
        try (FileOutputStream outputStream = new FileOutputStream(jobGraphFile)) {
            outputStream.write(bytes);
        }
        List<File> dependFiles = ImmutableList.<File>builder()
                .addAll(job.getJars().stream()
                        .filter(x -> !x.getPath().startsWith(sparkHome) && !x.getPath().endsWith(jobGraphFile.getName()))
                        .map(x -> new File(x.getPath()))
                        .collect(Collectors.toList()))
                .add(jobGraphFile)
                .build().stream().collect(Collectors.toMap(File::getName, v -> v, (x, y) -> y)).values() //distinct
                .stream().sorted(Comparator.comparing(File::getPath)).collect(Collectors.toList());

        String distFiles = dependFiles.stream().map(File::getAbsolutePath).collect(Collectors.joining(","));
        if (StringUtils.isNotBlank(distFiles)) {
            sparkConf.set("spark.yarn.dist.jars", distFiles);   //上传配置文件
        }
        return jobGraphFile;
    }

    private String[] getArgs(File jobGraphFile)
    {
        return new String[] {
                //"--name",
                //"test-SparkPi",

                //"--driver-memory",
                //"1000M",

                //"--jar", sparkExamplesJar,

                "--class", SparkAppMain.class.getName(),
                "--arg", jobGraphFile.getName()
                // argument 1 to my Spark program
                //"--arg", slices   user args

                // argument 2 to my Spark program (helper argument to create a proper JavaSparkContext object)
                //"--arg",
        };
    }
}
