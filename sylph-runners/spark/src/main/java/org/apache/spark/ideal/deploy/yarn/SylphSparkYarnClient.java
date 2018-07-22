package org.apache.spark.ideal.deploy.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

public class SylphSparkYarnClient
        extends Client
{
    // ApplicationMaster
    public SylphSparkYarnClient(ClientArguments clientArgs, SparkConf spConf)
    {
        super(clientArgs, spConf);
        SparkContext sparkContext;
        //ApplicationMaster.main();
    }

    @Override
    public ApplicationSubmissionContext createApplicationSubmissionContext(YarnClientApplication newApp, ContainerLaunchContext containerContext)
    {
        final ApplicationSubmissionContext appContext = super.createApplicationSubmissionContext(newApp, containerContext);
        appContext.setApplicationType("Ysera_SPARK");
        return appContext;
    }
}
