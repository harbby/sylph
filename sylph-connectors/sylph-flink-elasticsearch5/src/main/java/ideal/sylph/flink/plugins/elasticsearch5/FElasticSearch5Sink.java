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
package ideal.sylph.flink.plugins.elasticsearch5;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.Sink;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;


@Name("fes5sink")
@Description("this is elasticsearch5 sink plugin")
public class FElasticSearch5Sink
        implements Sink<DataStream<Row>> {

    private String clusterName;
    private String hosts;
    private String index;
    private String type;
    private  boolean update;
    private String idField;
    private int idIndex = -1;
    private final ideal.sylph.etl.Row.Schema schema;
    private final ElasticsearchSinkConfig config;

    public FElasticSearch5Sink(SinkContext context, ElasticsearchSinkConfig config) {

        this.clusterName=config.clusterName;
        this.hosts=config.hosts;
        this.index=config.index;
        this.type=config.type;
        this.update=config.update;
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
    public void run(DataStream<Row> stream){

        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", clusterName);
        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        try {
            for (String ip : hosts.split(",")) {
                transportAddresses.add(new InetSocketAddress(InetAddress.getByName(ip.split(":")[0]), Integer.valueOf(ip.split(":")[1])));
            }
        } catch (UnknownHostException e) {
                e.printStackTrace();
        }

        if (!update) {
            stream.addSink(new ElasticsearchSink<Row>(config, transportAddresses, new ElasticsearchSinkFunction<Row>() {
                public IndexRequest createIndexRequest(Row element) {
                    Map<String, Object> map = new HashMap<>();
                    for (int i=0;i<schema.getFieldNames().size();i++){
                        map.put(schema.getFieldNames().get(i), element.getField(i));
                    }

//                    for (String fieldName : schema.getFieldNames()) {
//                        map.put(fieldName, value.getAs(fieldName));
//                    }
                    return Requests.indexRequest()
                                .index(index)
                                .type(type)
                                .id(idField)
                                .source(map);
                }
                @Override
                public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            }));
        }else{
            stream.addSink(new ElasticsearchSink<Row>(config, transportAddresses, new ElasticsearchSinkFunction<Row>() {
                public IndexRequest createIndexRequest(Row element) {
                    Map<String, Object> map = new HashMap<>();
                    for (int i=0;i<schema.getFieldNames().size();i++){
                        map.put(schema.getFieldNames().get(i), element.getField(i));
                    }
                    return Requests.indexRequest()
                            .index(index)
                            .type(type)
                            .source(map);
                }
                @Override
                public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            }));
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
