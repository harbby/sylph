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
package com.github.harbby.sylph.colltroller;

import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.sylph.spi.SylphContext;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class ControllerApp
{
    private ServerConfig config;
    private SylphContext sylphContext;

    @Autowired
    public ControllerApp(
            Properties properties,
            SylphContext sylphContext)
            throws Exception
    {
        this.config = new ServerConfig(requireNonNull(properties, "config is null"));
        this.sylphContext = requireNonNull(sylphContext, "jobManager is null");
    }

    public void start()
            throws Exception
    {
        new JettyServer(config, sylphContext).start();
    }
}
