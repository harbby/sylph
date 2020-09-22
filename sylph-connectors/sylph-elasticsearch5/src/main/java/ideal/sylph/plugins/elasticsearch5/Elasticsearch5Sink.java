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

import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.Record;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

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
    private final Schema schema;
    private final ElasticsearchSinkConfig config;
    private final ClientFactory clientFactory;

    private TransportClient client;
    private int idIndex = -1;
    private final AtomicInteger cnt = new AtomicInteger(0);
    private BulkRequestBuilder bulkBuilder;

    @Autowired
    public Elasticsearch5Sink(SinkContext context, ElasticsearchSinkConfig config)
    {
        this.config = config;
        this.schema = context.getSchema();
        this.clientFactory = new ClientFactory(config);

        if (!Strings.isNullOrEmpty(config.getIdField())) {
            int fieldIndex = schema.getFieldIndex(config.getIdField());
            checkState(fieldIndex != -1, config.getIdField() + " does not exist, only " + schema.getFields());
            this.idIndex = fieldIndex;
        }
        if (config.isUpdate()) {
            checkState(idIndex != -1, "This is Update mode, `id_field` must be set");
        }
    }

    @Override
    public void process(Record value)
    {
        Map<String, Object> map = new HashMap<>();
        for (String fieldName : schema.getFieldNames()) {
            map.put(fieldName, value.getAs(fieldName));
        }
        if (config.isUpdate()) {  //is update
            Object id = value.getAs(idIndex);
            checkState(id != null, "this is  update mode,but id is null %s", value);
            UpdateRequestBuilder requestBuilder = client.prepareUpdate(config.getIndex(), config.getType(), id.toString());
            requestBuilder.setDoc(map);
            requestBuilder.setDocAsUpsert(true);
            bulkBuilder.add(requestBuilder.request());
        }
        else {
            IndexRequestBuilder requestBuilder = client.prepareIndex(config.getIndex(), config.getType());
            if (idIndex != -1) {
                Object id = value.getAs(idIndex);
                if (id != null) {
                    requestBuilder.setId(id.toString());
                }
            }

            requestBuilder.setSource(map);
            bulkBuilder.add(requestBuilder.request());
        }
        if (cnt.getAndIncrement() > config.getBatchSize()) {
            this.flush();
        }
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        TransportClient client = clientFactory.createClient();
        for (String ip : config.getHosts().split(",")) {
            client.addTransportAddress(new InetSocketTransportAddress(
                    InetAddress.getByName(ip.split(":")[0]), Integer.valueOf(ip.split(":")[1])));
        }
        this.client = client;
        this.bulkBuilder = client.prepareBulk();
        return true;
    }

    @Override
    public void flush()
    {
        client.bulk(bulkBuilder.request()).actionGet();
        cnt.set(0);
        bulkBuilder = client.prepareBulk();
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        if (errorOrNull != null) {
            errorOrNull.printStackTrace();
        }
        try (TransportClient closeClient = client) {
            if (bulkBuilder != null && closeClient != null) {
                this.flush();
            }
        }
    }
}
