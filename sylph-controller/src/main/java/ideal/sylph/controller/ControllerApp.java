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
package ideal.sylph.controller;

import com.google.inject.Inject;
import ideal.sylph.spi.SylphContext;

import static java.util.Objects.requireNonNull;

/**
 * 视图层目前 为实验功能
 */
public class ControllerApp
{
    private ServerConfig config;
    private SylphContext sylphContext;

    @Inject
    public ControllerApp(
            ServerConfig config,
            SylphContext sylphContext
    )
    {
        this.config = requireNonNull(config, "config is null");
        this.sylphContext = requireNonNull(sylphContext, "jobManager is null");
    }

    public void start()
            throws Exception
    {
        new JettyServer(config, sylphContext).start();
    }
}
