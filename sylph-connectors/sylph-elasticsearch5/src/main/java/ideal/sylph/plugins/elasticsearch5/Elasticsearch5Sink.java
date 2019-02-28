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
package ideal.sylph.plugins.elasticsearch5;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

@Name("elasticsearch5")
@Description("this is elasticsearch5 sink plugin")
public class Elasticsearch5Sink
        implements RealTimeSink
{
    private static final int MAX_BATCH_BULK = 50;
    private final Row.Schema schema;
    private final ElasticsearchSinkConfig config;

    private TransportClient client;
    private int idIndex = -1;
    private final AtomicInteger cnt = new AtomicInteger(0);
    private BulkRequestBuilder bulkBuilder;

    public Elasticsearch5Sink(SinkContext context, ElasticsearchSinkConfig config)
    {
        schema = context.getSchema();
        this.config = config;
        if (!Strings.isNullOrEmpty(config.idField)) {
            int fieldIndex = schema.getFieldIndex(config.idField);
            checkState(fieldIndex != -1, config.idField + " does not exist, only " + schema.getFields());
            this.idIndex = fieldIndex;
        }
        if (config.update) {
            checkState(idIndex != -1, "This is Update mode, `id_field` must be set");
        }
    }

    @Override
    public void process(Row value)
    {
        Map<String, Object> map = new HashMap<>();
        for (String fieldName : schema.getFieldNames()) {
            map.put(fieldName, value.getAs(fieldName));
        }
        if (config.update) {  //is update
            Object id = value.getAs(idIndex);
            if (id == null) {
                return;
            }
            UpdateRequestBuilder requestBuilder = client.prepareUpdate(config.index, config.type, id.toString());
            requestBuilder.setDoc(map);
            requestBuilder.setDocAsUpsert(true);
            bulkBuilder.add(requestBuilder.request());
        }
        else {
            IndexRequestBuilder requestBuilder = client.prepareIndex(config.index, config.type);
            if (idIndex != -1) {
                Object id = value.getAs(idIndex);
                if (id != null) {
                    requestBuilder.setId(id.toString());
                }
            }

            requestBuilder.setSource(map);
            bulkBuilder.add(requestBuilder.request());
        }
        if (cnt.getAndIncrement() > MAX_BATCH_BULK) {
            client.bulk(bulkBuilder.request()).actionGet();
            cnt.set(0);
            bulkBuilder = client.prepareBulk();
        }
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        String clusterName = config.clusterName;
        String hosts = config.hosts;
        Settings settings = Settings.builder().put("cluster.name", clusterName)
                .put("client.transport.sniff", true).build();

        TransportClient client = new PreBuiltTransportClient(settings);
        for (String ip : hosts.split(",")) {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip.split(":")[0]), Integer.valueOf(ip.split(":")[1])));
        }
        this.client = client;
        this.bulkBuilder = client.prepareBulk();
        return true;
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        try (TransportClient closeClient = client) {
            if (bulkBuilder != null && closeClient != null) {
                closeClient.bulk(bulkBuilder.request());
            }
        }
    }

    public static class ElasticsearchSinkConfig
            extends PluginConfig
    {
        @Name("cluster_name")
        @Description("this is es cluster name")
        private String clusterName;

        @Name("cluster_hosts")
        @Description("this is es cluster hosts")
        private String hosts;

        @Name("es_index")
        @Description("this is es index")
        private String index;

        @Name("id_field")
        @Description("this is es id_field")
        private String idField;

        @Name("update")
        @Description("update or insert")
        private boolean update = false;

        @Name("index_type")
        @Description("this is es index_type, Do not set")
        private String type = "default";
    }
}
