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
package ideal.sylph.plugins.elasticsearch6;

import com.github.harbby.gadtry.ioc.Autowired;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ClientFactory
{
    private final ElasticsearchSinkConfig config;
    private final Settings settings;

    @Autowired
    public ClientFactory(ElasticsearchSinkConfig config)
    {
        this.config = config;
        this.settings = Settings.builder().put("cluster.name", config.getClusterName())
                .put("client.transport.sniff", true).build();
    }

    public ElasticsearchSinkConfig getConfig()
    {
        return config;
    }

    public TransportClient createClient()
    {
        return new PreBuiltTransportClient(settings);
    }
}
