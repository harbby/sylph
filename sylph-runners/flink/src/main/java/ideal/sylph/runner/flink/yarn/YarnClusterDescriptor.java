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
package ideal.sylph.runner.flink.yarn;

import ideal.sylph.runner.flink.actuator.JobParameter;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.Utils;
import org.apache.flink.yarn.YarnApplicationMasterRunner;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.yarn.YarnConfigKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class YarnClusterDescriptor
        extends AbstractYarnClusterDescriptor
{
    private static final String APPLICATION_TYPE = "Sylph_FLINK";
    private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.class);
    private static final int MAX_ATTEMPT = 2;

    private final YarnClusterConfiguration clusterConf;
    private final YarnConfiguration yarnConfiguration;
    private final YarnClient yarnClient;
    private final JobParameter appConf;
    private final String jobName;
    private final Iterable<Path> userProvidedJars;

    private YarnClientApplication application;
    private Path flinkJar;

    YarnClusterDescriptor(
            final YarnClusterConfiguration clusterConf,
            final YarnClient yarnClient,
            final JobParameter appConf,
            String jobName,
            Iterable<Path> userProvidedJars)
    {
        super(clusterConf.flinkConfiguration(), clusterConf.yarnConf(), clusterConf.appRootDir(), yarnClient, false);
        this.jobName = jobName;
        this.clusterConf = clusterConf;
        this.yarnClient = yarnClient;
        this.appConf = appConf;
        this.userProvidedJars = userProvidedJars;
        this.yarnConfiguration = clusterConf.yarnConf();
    }

    @Override
    protected String getYarnSessionClusterEntrypoint()
    {
        return YarnApplicationMasterRunner.class.getName();
    }

    /**
     * 提交到yarn时 任务启动入口类
     */
    @Override
    protected String getYarnJobClusterEntrypoint()
    {
        return YarnApplicationMasterRunner.class.getName();
    }

    @Override
    protected ClusterClient<ApplicationId> createYarnClusterClient(
            AbstractYarnClusterDescriptor descriptor,
            int numberTaskManagers,
            int slotsPerTaskManager,
            ApplicationReport report,
            Configuration flinkConfiguration,
            boolean perJobCluster)
            throws Exception
    {
        return new RestClusterClient<>(
                flinkConfiguration,
                report.getApplicationId());
    }

    @Override
    public YarnClient getYarnClient()
    {
        return this.yarnClient;
    }

    public ClusterClient<ApplicationId> deploy()
    {
        try {
            YarnClientApplication application = yarnClient.createApplication();
            ApplicationReport report = startAppMaster(application);

            Configuration flinkConfiguration = getFlinkConfiguration();
            flinkConfiguration.setString(JobManagerOptions.ADDRESS.key(), report.getHost());
            flinkConfiguration.setInteger(JobManagerOptions.PORT.key(), report.getRpcPort());

            flinkConfiguration.setString(RestOptions.ADDRESS, report.getHost());
            flinkConfiguration.setInteger(RestOptions.PORT, report.getRpcPort());

            //return new RestClusterClient<>(flinkConfiguration, report.getApplicationId()).getMaxSlots();
            return new YarnClusterClient(this,
                    appConf.getTaskManagerCount(),
                    appConf.getTaskManagerSlots(),
                    report, clusterConf.flinkConfiguration(), false);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ApplicationReport startAppMaster(YarnClientApplication application)
            throws Exception
    {
        ApplicationSubmissionContext appContext = application.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setMaxAppAttempts(MAX_ATTEMPT);

        Path appHomeDir = new Path(clusterConf.appRootDir(), appId.toString());

        Map<String, LocalResource> localResources = new HashMap<>();
        Set<Path> shippedPaths = new HashSet<>();
        collectLocalResources(appHomeDir, localResources, shippedPaths);

        final ContainerLaunchContext amContainer = setupApplicationMasterContainer(
                getYarnJobClusterEntrypoint(),
                false,
                true,
                false,
                appConf.getJobManagerMemoryMb()
        );

        amContainer.setLocalResources(localResources);

        final String classPath = String.join(File.pathSeparator, localResources.keySet());

        final String shippedFiles = shippedPaths.stream()
                .map(path -> path.getName() + "=" + path)
                .collect(Collectors.joining(","));

        // Setup CLASSPATH and environment variables for ApplicationMaster
        final Map<String, String> appMasterEnv = setUpAmEnvironment(
                appHomeDir,
                appId,
                classPath,
                shippedFiles,
                getDynamicPropertiesEncoded()
        );

        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(appConf.getJobManagerMemoryMb());  //设置jobManneger
        capability.setVirtualCores(1);  //default 1

        appContext.setApplicationName(jobName);
        appContext.setApplicationType(APPLICATION_TYPE);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setApplicationTags(appConf.getAppTags());
        if (appConf.getQueue() != null) {
            appContext.setQueue(appConf.getQueue());
        }

        // add a hook to clean up in case deployment fails
        Path yarnAppDir = new Path(clusterConf.appRootDir(), appContext.getApplicationId().toString());
        Thread deploymentFailureHook = new DeploymentFailureHook(yarnClient, application, yarnAppDir);
        Runtime.getRuntime().addShutdownHook(deploymentFailureHook);
        LOG.info("Submitting application master {}", appId);
        yarnClient.submitApplication(appContext);

        LOG.info("Waiting for the cluster to be allocated");
        final long startTime = System.currentTimeMillis();
        ApplicationReport report;
        YarnApplicationState lastAppState = YarnApplicationState.NEW;
        loop:
        while (true) {
            try {
                report = yarnClient.getApplicationReport(appId);
            }
            catch (IOException e) {
                throw new YarnDeploymentException("Failed to deploy the cluster.", e);
            }
            YarnApplicationState appState = report.getYarnApplicationState();
            LOG.debug("Application State: {}", appState);
            switch (appState) {
                case FAILED:
                case FINISHED:
                case KILLED:
                    throw new YarnDeploymentException("The YARN application unexpectedly switched to state "
                            + appState + " during deployment. \n" +
                            "Diagnostics from YARN: " + report.getDiagnostics() + "\n" +
                            "If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n" +
                            "yarn logs -applicationId " + appId);
                    //break ..
                case RUNNING:
                    LOG.info("YARN application has been deployed successfully.");
                    break loop;
                default:
                    if (appState != lastAppState) {
                        LOG.info("Deploying cluster, current state " + appState);
                    }
                    if (System.currentTimeMillis() - startTime > 60000) {
                        LOG.info("Deployment took more than 60 seconds. Please check if the requested resources are available in the YARN cluster");
                    }
            }
            lastAppState = appState;
            Thread.sleep(250);
        }
        // print the application id for user to cancel themselves.
        if (isDetachedMode()) {
            LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
                    "Flink on YARN, use the following command or a YARN web interface to stop " +
                    "it:\nyarn application -kill " + appId + "\nPlease also note that the " +
                    "temporary files of the YARN session in the home directory will not be removed.");
        }
        // since deployment was successful, remove the hook
        ShutdownHookUtil.removeShutdownHook(deploymentFailureHook, getClass().getSimpleName(), LOG);
        return report;
    }

    private void collectLocalResources(
            Path appHomeDir,
            Map<String, LocalResource> resources,
            Set<Path> shippedPaths
    )
            throws IOException, URISyntaxException
    {
        Path flinkJar = clusterConf.flinkJar();
        LocalResource flinkJarResource = setupLocalResource(flinkJar, appHomeDir, ""); //放到 Appid/根目录下
        this.flinkJar = ConverterUtils.getPathFromYarnURL(flinkJarResource.getResource());
        resources.put("flink.jar", flinkJarResource);

        for (Path p : clusterConf.resourcesToLocalize()) {  //主要是 flink.jar log4f.propors 和 flink.yaml 三个文件
            LocalResource resource = setupLocalResource(p, appHomeDir, ""); //这些需要放到根目录下
            resources.put(p.getName(), resource);
            if ("log4j.properties".equals(p.getName())) {
                shippedPaths.add(ConverterUtils.getPathFromYarnURL(resource.getResource()));
            }
        }

        for (Path p : userProvidedJars) {
            String name = p.getName();
            if (resources.containsKey(name)) {   //这里当jar 有重复的时候 会抛出异常
                LOG.warn("Duplicated name in the shipped files {}", p);
            }
            else {
                LocalResource resource = setupLocalResource(p, appHomeDir, "jars"); //这些放到 jars目录下
                resources.put(name, resource);
                shippedPaths.add(ConverterUtils.getPathFromYarnURL(resource.getResource()));
            }
        }
    }

    private LocalResource registerLocalResource(FileSystem fs, Path remoteRsrcPath)
            throws IOException
    {
        LocalResource localResource = Records.newRecord(LocalResource.class);
        FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
        localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
        localResource.setSize(jarStat.getLen());
        localResource.setTimestamp(jarStat.getModificationTime());
        localResource.setType(LocalResourceType.FILE);
        localResource.setVisibility(LocalResourceVisibility.APPLICATION);
        return localResource;
    }

    private LocalResource setupLocalResource(
            Path localSrcPath,
            Path homedir,
            String relativeTargetPath)
            throws IOException
    {
        if (new File(localSrcPath.toUri().getPath()).isDirectory()) {
            throw new IllegalArgumentException("File to copy must not be a directory: " +
                    localSrcPath);
        }

        // copy resource to HDFS
        String suffix = "." + (relativeTargetPath.isEmpty() ? "" : "/" + relativeTargetPath)
                + "/" + localSrcPath.getName();

        Path dst = new Path(homedir, suffix);

        LOG.info("Uploading {}", dst);

        FileSystem hdfs = FileSystem.get(clusterConf.yarnConf());
        hdfs.copyFromLocalFile(false, true, localSrcPath, dst);

        // now create the resource instance
        LocalResource resource = registerLocalResource(hdfs, dst);
        return resource;
    }

    private Map<String, String> setUpAmEnvironment(
            Path appHomeDir,
            ApplicationId appId,
            String amClassPath,
            String shipFiles,
            String dynamicProperties)
            throws IOException
    {
        final Map<String, String> appMasterEnv = new HashMap<>();

        // set Flink app class path
        appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, amClassPath);

        // set Flink on YARN internal configuration values
        appMasterEnv.put(YarnConfigKeys.ENV_TM_COUNT, String.valueOf(appConf.getTaskManagerCount()));
        appMasterEnv.put(YarnConfigKeys.ENV_TM_MEMORY, String.valueOf(appConf.getTaskManagerMemoryMb()));
        appMasterEnv.put(YarnConfigKeys.FLINK_JAR_PATH, flinkJar.toString());
        appMasterEnv.put(YarnConfigKeys.ENV_APP_ID, appId.toString());
        appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, appHomeDir.toString()); //$home/.flink/appid 这个目录里面存放临时数据
        appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, shipFiles);
        appMasterEnv.put(YarnConfigKeys.ENV_SLOTS, String.valueOf(appConf.getTaskManagerSlots()));
        appMasterEnv.put(YarnConfigKeys.ENV_DETACHED, String.valueOf(true));  //是否分离 分离就cluser模式 否则是client模式

        // https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md#identity-on-an-insecure-cluster-hadoop_user_name
        appMasterEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME,
                UserGroupInformation.getCurrentUser().getUserName());

        if (dynamicProperties != null) {
            appMasterEnv.put(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicProperties);
        }

        // set classpath from YARN configuration
        Utils.setupYarnClassPath(clusterConf.yarnConf(), appMasterEnv);

        return appMasterEnv;
    }

    /**
     * flink 1.5 add
     */
    @Override
    public ClusterClient<ApplicationId> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached)
            throws ClusterDeploymentException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    private static class YarnDeploymentException
            extends RuntimeException
    {
        private static final long serialVersionUID = -812040641215388943L;

        public YarnDeploymentException(String message)
        {
            super(message);
        }

        public YarnDeploymentException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    private class DeploymentFailureHook
            extends Thread
    {

        private final YarnClient yarnClient;
        private final YarnClientApplication yarnApplication;
        private final Path yarnFilesDir;

        DeploymentFailureHook(YarnClient yarnClient, YarnClientApplication yarnApplication, Path yarnFilesDir)
        {
            this.yarnClient = Preconditions.checkNotNull(yarnClient);
            this.yarnApplication = Preconditions.checkNotNull(yarnApplication);
            this.yarnFilesDir = Preconditions.checkNotNull(yarnFilesDir);
        }

        @Override
        public void run()
        {
            LOG.info("Cancelling deployment from Deployment Failure Hook");
            failSessionDuringDeployment(yarnClient, yarnApplication);
            LOG.info("Deleting files in {}.", yarnFilesDir);
            try {
                FileSystem fs = FileSystem.get(yarnConfiguration);

                if (!fs.delete(yarnFilesDir, true)) {
                    throw new IOException("Deleting files in " + yarnFilesDir + " was unsuccessful");
                }

                fs.close();
            }
            catch (IOException e) {
                LOG.error("Failed to delete Flink Jar and configuration files in HDFS", e);
            }
        }

        /**
         * Kills YARN application and stops YARN client.
         *
         * <p>Use this method to kill the App before it has been properly deployed
         */
        private void failSessionDuringDeployment(YarnClient yarnClient, YarnClientApplication yarnApplication)
        {
            LOG.info("Killing YARN application");

            try {
                yarnClient.killApplication(yarnApplication.getNewApplicationResponse().getApplicationId());
            }
            catch (Exception e) {
                // we only log a debug message here because the "killApplication" call is a best-effort
                // call (we don't know if the application has been deployed when the error occured).
                LOG.debug("Error while killing YARN application", e);
            }
            yarnClient.stop();
        }
    }
}
