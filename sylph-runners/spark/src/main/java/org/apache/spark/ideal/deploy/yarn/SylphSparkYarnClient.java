package org.apache.spark.ideal.deploy.yarn;

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
    // ApplicationMaster
    public SylphSparkYarnClient(ClientArguments clientArgs, SparkConf spConf, YarnClient yarnClient)
            throws NoSuchFieldException, IllegalAccessException
    {
        super(clientArgs, spConf);
        Field field = this.getClass().getSuperclass().getDeclaredField("org$apache$spark$deploy$yarn$Client$$hadoopConf");
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
        return appContext;
    }
}
