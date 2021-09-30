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
package com.github.harbby.sylph.colltroller.action;

import com.github.harbby.gadtry.collection.MutableMap;
import com.github.harbby.gadtry.spi.Module;
import com.github.harbby.sylph.api.Plugin;
import com.github.harbby.sylph.spi.SylphContext;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@jakarta.inject.Singleton
@Path("/plugin")
public class PluginManagerResource
{
    private final SylphContext sylphContext;

    public PluginManagerResource(@Context ServletContext servletContext)
    {
        this.sylphContext = (SylphContext) servletContext.getAttribute("sylphContext");
    }

    @Path("list_actuators")
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<String> getETLActuators()
    {
        return sylphContext.getAllEngineNames();
    }

    @GET
    @Path("actuator")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Map getAllPlugins(@QueryParam("actuator") String actuator)
    {
        checkArgument(!Strings.isNullOrEmpty(actuator), "actuator [" + actuator + "] not setting");
        return sylphContext.getEnginePlugins(actuator).stream().map(pluginInfo -> {
            Map config = pluginInfo.getPluginConfig().stream()
                    .collect(Collectors.toMap(
                            //todo: default value is ?
                            k -> k.get("key"), v -> v.get("default")));

            return ImmutableMap.<String, Object>builder()
                    .put("name", pluginInfo.getName())
                    .put("driver", pluginInfo.getDriverClass())
                    .put("description", pluginInfo.getDescription())
                    .put("version", pluginInfo.getVersion())
                    .put("realTime", pluginInfo.isRealTime())
                    .put("type", pluginInfo.getPipelineType())
                    .put("config", config)
                    .build();
        }).collect(Collectors.groupingBy(x -> x.get("type").toString().toLowerCase()));
    }

    @GET
    @Path("reload")
    public void reload()
            throws IOException
    {
        sylphContext.reload();
    }

    @GET
    @Path("delete_module")
    public void deleteModule(@QueryParam("name") String moduleName)
            throws IOException
    {
        sylphContext.deleteModule(moduleName);
    }

    @GET
    @Path("list_connectors")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Map getAllConnectors()
    {
        return sylphContext.getAllConnectors().stream().map(pluginInfo -> {
            Map config = pluginInfo.getPluginConfig().stream()
                    .collect(Collectors.toMap(
                            //todo: default value is ?
                            k -> k.get("key"), v -> v.get("default")));

            return ImmutableMap.<String, Object>builder()
                    .put("name", pluginInfo.getName())
                    .put("driver", pluginInfo.getDriverClass())
                    .put("description", pluginInfo.getDescription())
                    .put("version", pluginInfo.getVersion())
                    .put("realTime", pluginInfo.isRealTime() + "")
                    .put("type", pluginInfo.getPipelineType())
                    .put("config", config)
                    .build();
        }).collect(Collectors.groupingBy(x -> x.get("type").toString().toLowerCase()));
    }

    @GET
    @Path("list_modules")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List getAllConnectorModules()
    {
        List<Module<Plugin>> modules = sylphContext.getAllConnectorModules();
        return modules.stream().map(module -> {
            List<Class<?>> drivers = module.getPlugins().stream().flatMap(x -> x.getConnectors().stream()).collect(Collectors.toList());
            return MutableMap.<String, Object>builder()
                    .put("name", module.getName())
                    .put("path", module.moduleFile())
                    .put("loadTime", module.getLoadTime())
                    .put("size", drivers.size())
                    .put("drivers", drivers)
                    .build();
        }).collect(Collectors.toList());
    }
}
