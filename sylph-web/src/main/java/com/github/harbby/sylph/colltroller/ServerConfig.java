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

import java.util.Properties;

public class ServerConfig
{
    private final int serverPort;
    private final int maxFormContentSize;

    public ServerConfig(Properties properties)
    {
        this.serverPort = Integer.parseInt(properties.getProperty("web.server.port", "8080"));
        this.maxFormContentSize = Integer.parseInt(properties.getProperty("server.http.maxFormContentSize", "100"));
    }

    public int getServerPort()
    {
        return serverPort;
    }

    public int getMaxFormContentSize()
    {
        return maxFormContentSize;
    }
}
