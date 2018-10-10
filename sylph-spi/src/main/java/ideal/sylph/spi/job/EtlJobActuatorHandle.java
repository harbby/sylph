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
package ideal.sylph.spi.job;

import com.google.common.collect.ImmutableSet;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.commons.io.FileUtils;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public abstract class EtlJobActuatorHandle
        implements JobActuatorHandle
{
    @NotNull
    @Override
    public Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return EtlFlow.load(flowBytes);
    }

    @NotNull
    @Override
    public Collection<File> parserFlowDepends(Flow inFlow)
            throws IOException
    {
        EtlFlow flow = (EtlFlow) inFlow;
        //---- flow parser depends ----
        ImmutableSet.Builder<File> builder = ImmutableSet.builder();
        for (NodeInfo nodeInfo : flow.getNodes()) {
            String driverString = nodeInfo.getDriverClass();
            Optional<PipelinePluginManager.PipelinePluginInfo> pluginInfo = this.getPluginManager().findPluginInfo(driverString);
            pluginInfo.ifPresent(plugin -> FileUtils.listFiles(plugin.getPluginFile(), null, true)
                    .forEach(builder::add));
        }
        return builder.build();
    }
}
