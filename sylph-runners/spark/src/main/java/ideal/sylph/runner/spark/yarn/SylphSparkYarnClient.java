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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

import java.lang.reflect.Field;

public class SylphSparkYarnClient
        extends Client
{
    private final String yarnQueue;

    // ApplicationMaster
    public SylphSparkYarnClient(ClientArguments clientArgs, SparkConf sparkConf, YarnClient yarnClient, String yarnQueue)
            throws NoSuchFieldException, IllegalAccessException
    {
        super(clientArgs, sparkConf, null);
        this.yarnQueue = yarnQueue;

        //String key = DRIVER_MEMORY; //test
        //Field field = Client.class.getDeclaredField("org$apache$spark$deploy$yarn$Client$$hadoopConf"); //scala 2.11
        Field field = Client.class.getDeclaredField("hadoopConf");    //scala 2.12
        field.setAccessible(true);
        YarnConfiguration yarnConfiguration = new YarnConfiguration(yarnClient.getConfig());
        field.set(this, yarnConfiguration);
    }

    @Override
    public ApplicationSubmissionContext createApplicationSubmissionContext(YarnClientApplication newApp, ContainerLaunchContext containerContext)
    {
        final ApplicationSubmissionContext appContext = super.createApplicationSubmissionContext(newApp, containerContext);
        appContext.setApplicationType("Sylph_SPARK");
        appContext.setApplicationTags(ImmutableSet.of("a1", "a2"));
        appContext.setQueue(yarnQueue);
        appContext.setMaxAppAttempts(2);
        return appContext;
    }
}
