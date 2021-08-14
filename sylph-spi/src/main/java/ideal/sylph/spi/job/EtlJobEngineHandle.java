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

import com.github.harbby.gadtry.ioc.Autowired;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.etl.OperatorType;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.model.OperatorInfo;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class EtlJobEngineHandle
        implements JobEngineHandle
{
    private final RunnerContext runnerContext;

    @Autowired
    protected EtlJobEngineHandle(RunnerContext runnerContext)
    {
        this.runnerContext = requireNonNull(runnerContext, "runnerContext is null");
    }

    @NotNull
    @Override
    public Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return EtlFlow.load(flowBytes);
    }

    @NotNull
    @Override
    public Collection<OperatorInfo> parserFlowDepends(Flow inFlow)
            throws IOException
    {
        EtlFlow flow = (EtlFlow) inFlow;
        //---- flow parser depends ----
        ImmutableSet.Builder<OperatorInfo> builder = ImmutableSet.builder();
        for (NodeInfo nodeInfo : flow.getNodes()) {
            String driverOrName = nodeInfo.getDriverClass();
            OperatorType type = OperatorType.valueOf(nodeInfo.getNodeType());
            Optional<OperatorInfo> pluginInfo = runnerContext.getLatestMetaData(this).findConnectorInfo(driverOrName, type);
            pluginInfo.ifPresent(builder::add);
        }
        return builder.build();
    }
}
