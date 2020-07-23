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

import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.runner.flink.FlinkRunner;
import ideal.sylph.spi.job.Job;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 */
public class FlinkYarnJobLauncher {
    private static final Logger logger = LoggerFactory.getLogger(FlinkYarnJobLauncher.class);

    @Autowired
    private FlinkConfiguration flinkConf;
    @Autowired
    private YarnClient yarnClient;
    @Autowired
    YarnConfiguration yarnConfiguration;

    public YarnClient getYarnClient() {
        return yarnClient;
    }

    public ApplicationId start(Job job)
            throws Exception {
        JobGraph jobGraph = job.getJobDAG();

        List<File> userProvidedJars = getUserAdditionalJars(job.getDepends());
        final YarnJobDescriptor descriptor = new YarnJobDescriptor(
                flinkConf,
                yarnClient,
                yarnConfiguration,
                job.getConfig(),
                job.getName());
        descriptor.addShipFiles(userProvidedJars);

        try {
            logger.info("start flink job {}", jobGraph.getJobID());
            ClusterClient<ApplicationId> client = descriptor.deploy(jobGraph, true);  //create yarn appMaster
            ApplicationId applicationId = client.getClusterId();
            client.close();
            return applicationId;
        } catch (Throwable e) {
            logger.error("submitting job {} failed", jobGraph.getJobID(), e);
            throw e;
        }
    }

    private static List<File> getUserAdditionalJars(Collection<URL> userJars) {
        return userJars.stream().map(jar -> {
            try {
                final URI uri = jar.toURI();
                final File file = new File(uri);
                if (file.exists() && file.isFile()) {
                    return file;
                }
            } catch (Exception e) {
                logger.warn("add user jar error with URISyntaxException {}", jar);
            }
            return null;
        }).filter(x -> Objects.nonNull(x) && !x.getName().startsWith(FlinkRunner.FLINK_DIST)).collect(Collectors.toList());
    }
}
