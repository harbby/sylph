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
package ideal.sylph.runner.flink;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import ideal.sylph.runner.flink.actuator.FlinkStreamEtlActuator;
import ideal.sylph.runner.flink.actuator.FlinkStreamSqlActuator;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.runner.flink.FlinkRunner.FLINK_DIST;
import static java.util.Objects.requireNonNull;

public class FlinkRunnerModule
        implements Module
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkRunnerModule.class);

    @Override
    public void configure(Binder binder)
    {
        binder.bind(FlinkStreamEtlActuator.class).in(Scopes.SINGLETON);
        binder.bind(FlinkStreamSqlActuator.class).in(Scopes.SINGLETON);
    }
}
