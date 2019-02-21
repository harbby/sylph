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

import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.FlinkRunner;
import ideal.sylph.runner.flink.actuator.JobParameter;
import ideal.sylph.spi.job.Job;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 */
public class FlinkYarnJobLauncher
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkYarnJobLauncher.class);

    @Autowired
    private FlinkConfiguration flinkConf;
    @Autowired
    private YarnClient yarnClient;
    @Autowired YarnConfiguration yarnConfiguration;

    public YarnClient getYarnClient()
    {
        return yarnClient;
    }

    public Optional<ApplicationId> start(Job job)
            throws Exception
    {
        JobGraph jobGraph = ((FlinkJobHandle) job.getJobHandle()).getJobGraph();
        JobParameter jobConfig = ((FlinkJobConfig) job.getConfig()).getConfig();

        Iterable<Path> userProvidedJars = getUserAdditionalJars(job.getDepends());
        final YarnJobDescriptor descriptor = new YarnJobDescriptor(
                flinkConf,
                yarnClient,
                yarnConfiguration,
                jobConfig,
                job.getId(),
                userProvidedJars);
        return start(descriptor, jobGraph);
    }

    private Optional<ApplicationId> start(YarnJobDescriptor descriptor, JobGraph job)
            throws Exception
    {
        ApplicationId applicationId = null;
        try {
            logger.info("start flink job {}", job.getJobID());
            ClusterClient<ApplicationId> client = descriptor.deploy(job, true);  //create yarn appMaster
            applicationId = client.getClusterId();
            client.shutdown();
            return Optional.of(applicationId);
        }
        catch (Exception e) {
            if (applicationId != null) {
                yarnClient.killApplication(applicationId);
            }
            Thread thread = Thread.currentThread();
            if (e instanceof InterruptedIOException ||
                    thread.isInterrupted() ||
                    Throwables.getRootCause(e) instanceof InterruptedException) {
                logger.warn("job {} Canceled submission", job.getJobID());
                return Optional.empty();
            }
            else {
                throw e;
            }
        }
    }

    private static Iterable<Path> getUserAdditionalJars(Collection<URL> userJars)
    {
        return userJars.stream().map(jar -> {
            try {
                final URI uri = jar.toURI();
                final File file = new File(uri);
                if (file.exists() && file.isFile()) {
                    return new Path(uri);
                }
            }
            catch (Exception e) {
                logger.warn("add user jar error with URISyntaxException {}", jar);
            }
            return null;
        }).filter(x -> Objects.nonNull(x) && !x.getName().startsWith(FlinkRunner.FLINK_DIST)).collect(Collectors.toList());
    }
}
