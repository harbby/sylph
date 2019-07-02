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
package ideal.sylph.spi;

import com.github.harbby.gadtry.collection.mutable.MutableSet;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.spi.model.ConnectorInfo;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static java.util.Objects.requireNonNull;

public class ConnectorStore
        implements Serializable
{
    private final Map<String, ConnectorInfo> connectorMap = new HashMap<>();

    public ConnectorStore(Set<ConnectorInfo> connectors)
    {
        for (ConnectorInfo info : connectors) {
            MutableSet.<String>builder().addAll(info.getNames()).add(info.getDriverClass())
                    .build()
                    .forEach(name -> connectorMap.put(name + "\u0001" + info.getPipelineType(), info));
        }
    }

    /**
     * use test
     */
    public static ConnectorStore getDefault()
    {
        return new ConnectorStore(Collections.emptySet())
        {
            @Override
            public <T> Class<T> getConnectorDriver(String driverOrName, PipelinePlugin.PipelineType pipelineType)
            {
                try {
                    return (Class<T>) Class.forName(driverOrName);
                }
                catch (ClassNotFoundException e) {
                    throw throwsException(e);
                }
            }
        };
    }

    public <T> Class<T> getConnectorDriver(String driverOrName, PipelinePlugin.PipelineType pipelineType)
    {
        requireNonNull(driverOrName, "driverOrName is null");
        try {
            String driver = findConnectorInfo(driverOrName, pipelineType).map(x -> x.getDriverClass())
                    .orElse(driverOrName);
            return (Class<T>) Class.forName(driver);
        }
        catch (ClassNotFoundException e) {
            throw throwsException(e);
        }
    }

    public int size()
    {
        return (int) connectorMap.values().stream().distinct().count();
    }

    public Optional<ConnectorInfo> findConnectorInfo(String driverOrName, PipelinePlugin.PipelineType pipelineType)
    {
        requireNonNull(pipelineType, "pipelineType is null");
        return Optional.ofNullable(connectorMap.get(driverOrName + "\u0001" + pipelineType));
    }
}
