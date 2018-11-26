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
package ideal.sylph.runner.spark.yarn;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import ideal.common.base.Serializables;
import ideal.sylph.runner.spark.SparkJobHandle;
import ideal.sylph.spi.job.Job;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.spark.ideal.deploy.yarn.SylphSparkYarnClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SparkAppLauncher
{
    @Inject private YarnClient yarnClient;
    private static final String sparkHome = System.getenv("SPARK_HOME");

    public YarnClient getYarnClient()
    {
        return yarnClient;
    }

    public ApplicationId run(Job job)
            throws Exception
    {
        System.setProperty("SPARK_YARN_MODE", "true");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setSparkHome(sparkHome);

        sparkConf.setMaster("yarn");
        sparkConf.setAppName(job.getId());

        sparkConf.set("spark.submit.deployMode", "cluster"); // worked
        //set Depends set spark.yarn.dist.jars and spark.yarn.dist.files
        setDistJars(job, sparkConf);

        String[] args = getArgs();
        ClientArguments clientArguments = new ClientArguments(args);   // spark-2.0.0
        //yarnClient.getConfig().iterator().forEachRemaining(x -> sparkConf.set("spark.hadoop." + x.getKey(), x.getValue()));
        Client appClient = new SylphSparkYarnClient(clientArguments, sparkConf, yarnClient);
        return appClient.submitApplication();
    }

    private static void setDistJars(Job job, SparkConf sparkConf)
            throws IOException
    {
        File byt = new File(job.getWorkDir(), "job_handle.byt");
        byte[] bytes = Serializables.serialize((SparkJobHandle) job.getJobHandle());
        try (FileOutputStream outputStream = new FileOutputStream(byt)) {
            outputStream.write(bytes);
        }
        List<File> dependFiles = ImmutableList.<File>builder()
                .addAll(job.getDepends().stream()
                        .filter(x -> !x.getPath().startsWith(sparkHome) && !x.getPath().endsWith(byt.getName()))
                        .map(x -> new File(x.getPath()))
                        .collect(Collectors.toList()))
                .add(byt)
                .build().stream().collect(Collectors.toMap(File::getName, v -> v, (x, y) -> y)).values() //distinct
                .stream().sorted(Comparator.comparing(File::getPath)).collect(Collectors.toList());

        String distFiles = dependFiles.stream().map(File::getAbsolutePath).collect(Collectors.joining(","));
        if (StringUtils.isNotBlank(distFiles)) {
            sparkConf.set("spark.yarn.dist.jars", distFiles);   //上传配置文件
        }
    }

    private String[] getArgs()
    {
        return new String[] {
                //"--name",
                //"test-SparkPi",

                //"--driver-memory",
                //"1000M",

                //"--jar", sparkExamplesJar,

                "--class", ideal.sylph.runner.spark.SparkAppMain.class.getName(),

                // argument 1 to my Spark program
                //"--arg", slices   用户自定义的参数

                // argument 2 to my Spark program (helper argument to create a proper JavaSparkContext object)
                //"--arg",
        };
    }
}
