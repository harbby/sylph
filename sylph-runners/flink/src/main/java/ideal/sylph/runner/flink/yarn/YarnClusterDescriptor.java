package ideal.sylph.runner.flink.yarn;

import ideal.sylph.runner.flink.JobParameter;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
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
import org.apache.hadoop.yarn.exceptions.YarnException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.NEW;

public class YarnClusterDescriptor
        extends AbstractYarnClusterDescriptor
{
    private static final String APPLICATION_TYPE = "Ysera";
    private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.class);
    private static final int MAX_ATTEMPT = 1;
    private static final long DEPLOY_TIMEOUT_MS = 600 * 1000;
    private static final long RETRY_DELAY_MS = 250;
    private static final ScheduledExecutorService YARN_POLL_EXECUTOR = Executors.newSingleThreadScheduledExecutor();
    private static final String FLINK_JAR = "flink.jar";

    private final YarnClusterConfiguration clusterConf;
    private final YarnClient yarnClient;
    private final JobParameter appConf;
    private final Path homedir;
    private final ApplicationId yarnAppId;

    private Path flinkJar;

    /**
     * 构造函数
     */
    YarnClusterDescriptor(
            final YarnClusterConfiguration clusterConf,
            final YarnClient yarnClient,
            final JobParameter appConf,
            ApplicationId yarnAppId)
    {
        super(clusterConf.flinkConfiguration(), clusterConf.conf(), clusterConf.appRootDir(), yarnClient, false);

        this.clusterConf = clusterConf;
        this.yarnClient = yarnClient;
        this.appConf = appConf;
        this.yarnAppId = yarnAppId;
        this.homedir = new Path(clusterConf.appRootDir(), yarnAppId.toString());
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
    protected ClusterClient<ApplicationId> createYarnClusterClient(AbstractYarnClusterDescriptor descriptor, int numberTaskManagers, int slotsPerTaskManager, ApplicationReport report, Configuration flinkConfiguration, boolean perJobCluster)
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

    public YarnClusterClient deploy()
    {
        ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
        context.setApplicationId(yarnAppId);  //设置yarn上面显示的信息

        try {
            ApplicationReport report = startAppMaster(context);

            Configuration conf = getFlinkConfiguration();
            conf.setString(JobManagerOptions.ADDRESS.key(), report.getHost());
            conf.setInteger(JobManagerOptions.PORT.key(), report.getRpcPort());

            return new YarnClusterClient(this,
                    appConf.getTaskManagerCount(),
                    appConf.getTaskManagerSlots(),
                    report, conf, false);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ApplicationReport startAppMaster(ApplicationSubmissionContext appContext)
            throws Exception
    {
        ApplicationId appId = appContext.getApplicationId();
        appContext.setMaxAppAttempts(MAX_ATTEMPT);

        Map<String, LocalResource> localResources = new HashMap<>();
        Set<Path> shippedPaths = new HashSet<>();
        collectLocalResources(localResources, shippedPaths);

        final ContainerLaunchContext amContainer = setupApplicationMasterContainer(
                getYarnJobClusterEntrypoint(),
                false,
                true,
                false,
                appConf.getJobManagerMemoryMb()
        );

        amContainer.setLocalResources(localResources);

        final String classPath = localResources.keySet().stream()
                .collect(Collectors.joining(File.pathSeparator));

        final String shippedFiles = shippedPaths.stream()
                .map(path -> path.getName() + "=" + path)
                .collect(Collectors.joining(","));

        // Setup CLASSPATH and environment variables for ApplicationMaster
        final Map<String, String> appMasterEnv = setUpAmEnvironment(
                appId,
                classPath,
                shippedFiles,
                getDynamicPropertiesEncoded()
        );

        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(appConf.getJobManagerMemoryMb());  //设置jobManneger
        capability.setVirtualCores(1);  //默认是1

        appContext.setApplicationName(appConf.getYarnJobName());
        appContext.setApplicationType(APPLICATION_TYPE);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setApplicationTags(appConf.getAppTags());
        if (appConf.getQueue() != null) {
            appContext.setQueue(appConf.getQueue());
        }

        LOG.info("Submitting application master {}", appId);
        yarnClient.submitApplication(appContext);

        PollDeploymentStatus poll = new PollDeploymentStatus(appId);
        YARN_POLL_EXECUTOR.submit(poll);
        try {
            return poll.result.get();
        }
        catch (ExecutionException e) {
            LOG.warn("Failed to deploy {}, cause: {}", appId.toString(), e.getCause());
            yarnClient.killApplication(appId);
            throw (Exception) e.getCause();
        }
    }

    private void collectLocalResources(
            Map<String, LocalResource> resources,
            Set<Path> shippedPaths
    )
            throws IOException, URISyntaxException
    {
        Path flinkJar = clusterConf.flinkJar();
        LocalResource flinkJarResource = setupLocalResource(flinkJar, homedir, ""); //这些需要放到根目录下
        this.flinkJar = ConverterUtils.getPathFromYarnURL(flinkJarResource.getResource());
        resources.put("flink.jar", flinkJarResource);

        for (Path p : clusterConf.resourcesToLocalize()) {  //主要是 flink.jar log4f.propors 和 flink.yaml 三个文件
            LocalResource resource = setupLocalResource(p, homedir, ""); //这些需要放到根目录下
            resources.put(p.getName(), resource);
            if ("log4j.properties".equals(p.getName())) {
                shippedPaths.add(ConverterUtils.getPathFromYarnURL(resource.getResource()));
            }
        }

        for (Path p : appConf.getUserProvidedJars()) {
            String name = p.getName();
            if (resources.containsKey(name)) {  //这里当jar 有重复的时候 会抛出异常
                LOG.warn("Duplicated name in the shipped files {}", p);
            }
            else {
                LocalResource resource = setupLocalResource(p, homedir, "jars"); //这些放到 jars目录下
                resources.put(name, resource);
                //resources.put(name, toLocalResource(p, LocalResourceVisibility.APPLICATION));
                shippedPaths.add(ConverterUtils.getPathFromYarnURL(resource.getResource()));
            }
        }
    }

    /**
     * 注册文件
     */
    private LocalResource toLocalResource(Path path, LocalResourceVisibility visibility)
            throws IOException
    {
        FileSystem fs = path.getFileSystem(clusterConf.conf());
        FileStatus stat = fs.getFileStatus(path);
        return LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(path),
                LocalResourceType.FILE,
                visibility,
                stat.getLen(), stat.getModificationTime()
        );
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

        //FileSystem hdfs = localSrcPath.getFileSystem(clusterConf.conf());
        FileSystem hdfs = FileSystem.get(clusterConf.conf());
        hdfs.copyFromLocalFile(false, true, localSrcPath, dst);

        // now create the resource instance
        LocalResource resource = registerLocalResource(hdfs, dst);
        return resource;
    }

    private Map<String, String> setUpAmEnvironment(
            ApplicationId appId,
            String amClassPath,
            String shipFiles,
            String dynamicProperties)
            throws IOException, URISyntaxException
    {
        final Map<String, String> appMasterEnv = new HashMap<>();

        // set Flink app class path
        appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, amClassPath);

        // set Flink on YARN internal configuration values
        appMasterEnv.put(YarnConfigKeys.ENV_TM_COUNT, String.valueOf(appConf.getTaskManagerCount()));
        appMasterEnv.put(YarnConfigKeys.ENV_TM_MEMORY, String.valueOf(appConf.getTaskManagerMemoryMb()));
        appMasterEnv.put(YarnConfigKeys.FLINK_JAR_PATH, flinkJar.toString());
        appMasterEnv.put(YarnConfigKeys.ENV_APP_ID, appId.toString());
        appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, homedir.toString()); //$home/.flink/appid 这个目录里面存放临时数据
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
        Utils.setupYarnClassPath(clusterConf.conf(), appMasterEnv);

        return appMasterEnv;
    }

    /**
     * flink 1.5 新增方法
     */
    @Override
    public ClusterClient<ApplicationId> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached)
            throws ClusterDeploymentException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    private final class PollDeploymentStatus
            implements Runnable
    {
        private final CompletableFuture<ApplicationReport> result = new CompletableFuture<>();
        private final ApplicationId appId;
        private YarnApplicationState lastAppState = NEW;
        private long startTime;

        private PollDeploymentStatus(ApplicationId appId)
        {
            this.appId = appId;
        }

        @Override
        public void run()
        {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();
            }

            try {
                ApplicationReport report = poll();
                if (report == null) {
                    YARN_POLL_EXECUTOR.schedule(this, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                }
                else {
                    result.complete(report);
                }
            }
            catch (YarnException | IOException e) {
                result.completeExceptionally(e);
            }
        }

        private ApplicationReport poll()
                throws IOException, YarnException
        {
            ApplicationReport report;
            report = yarnClient.getApplicationReport(appId);
            YarnApplicationState appState = report.getYarnApplicationState();
            LOG.debug("Application State: {}", appState);

            switch (appState) {
                case FAILED:
                case FINISHED:
                    //TODO: the finished state may be valid in flip-6
                case KILLED:
                    throw new IOException("The YARN application unexpectedly switched to state "
                            + appState + " during deployment. \n"
                            + "Diagnostics from YARN: " + report.getDiagnostics() + "\n"
                            + "If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n"
                            + "yarn logs -applicationId " + appId);
                    //break ..
                case RUNNING:
                    LOG.info("YARN application has been deployed successfully.");
                    break;
                default:
                    if (appState != lastAppState) {
                        LOG.info("Deploying cluster, current state " + appState);
                    }
                    lastAppState = appState;
                    if (System.currentTimeMillis() - startTime > DEPLOY_TIMEOUT_MS) {
                        throw new RuntimeException(String.format("Deployment took more than %d seconds. "
                                + "Please check if the requested resources are available in the YARN cluster", DEPLOY_TIMEOUT_MS));
                    }
                    return null;
            }
            return report;
        }
    }
}
