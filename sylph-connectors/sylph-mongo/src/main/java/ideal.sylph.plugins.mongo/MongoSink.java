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
package ideal.sylph.plugins.mongo;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

@Name("mongo")
@Description("this sylph mongo sink")
public class MongoSink
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(MongoSink.class);

    private MongoConf mongoConf;
    private transient MongoClient mongoClient;
    private transient MongoCollection mongoCollection;
    private Schema schema;
    private Map<String, String> fieldTypeMap;

    public MongoSink(SinkContext sinkContext, MongoConf mongoConf)
    {
        this.mongoConf = mongoConf;
        this.schema = sinkContext.getSchema();

        Map<String, String> fieldTypes = new HashMap<>(16);
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            fieldTypes.put(schema.getFieldNames().get(i), schema.getFieldTypes().get(i).toString().split(" ")[1]);
        }
        this.fieldTypeMap = fieldTypes;
    }

    @Override
    public void process(Row value)
    {
        Document document = new Document();
        for(String fieldName: schema.getFieldNames()) {
            document.put(fieldName, value.getAs(fieldName));
        }
        mongoCollection.insertOne(document);
    }

    @Override
    public boolean open(long partitionId, long version) throws Exception
    {
        MongoCredential mongoCredential = MongoCredential.createCredential(mongoConf.getUserName(),
                mongoConf.getDataBase(),
                mongoConf.getPassWord().toCharArray());
        mongoClient = new MongoClient(parseServerAddress(mongoConf.getServerAddress()), Arrays.asList(mongoCredential));
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongoConf.getDataBase());
        mongoCollection = mongoDatabase.getCollection(mongoConf.getCollection());
        return true;
    }

    @Override
    public void close(Throwable errorOrNull) {
        mongoClient.close();
    }

    /**
     * server address
     * @param serverAddress
     * @return
     */
    public  List<ServerAddress> parseServerAddress(String serverAddress)
    {
        List<ServerAddress> ServerAddressList = new ArrayList<>(16);
        String[] address = serverAddress.split(",");
        for (int i = 0; i < address.length; i++) {
            String[] serverPort = address[i].split(":");
            if (serverPort.length == 2) {
                ServerAddressList.add(new ServerAddress(serverPort[0], Integer.valueOf(serverPort[1])));
            }
        }
        return ServerAddressList;
    }

    public static final class MongoConf
            extends PluginConfig {
        @Name("serverAddress")
        @Description("this is mongo serverAddress")
        private String serverAddress;

        @Name("passWord")
        @Description("this is mongo passWord")
        private String passWord;

        @Name("userName")
        @Description("this is mongo userName")
        private String userName;

        @Name("dataBase")
        @Description("this is mongo dataBase")
        private String dataBase;

        @Name("collection")
        @Description("this is mongo collection")
        private String collection;

        public String getServerAddress() {
            return serverAddress;
        }

        public String getPassWord() {
            return passWord;
        }

        public String getUserName() {
            return userName;
        }

        public String getDataBase() {
            return dataBase;
        }

        public String getCollection() {
            return collection;
        }
    }
}