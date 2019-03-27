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
package ideal.sylph.plugins.hbase;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.plugins.hbase.tuple.Tuple2;
import ideal.sylph.plugins.hbase.util.BytesUtil;
import ideal.sylph.plugins.hbase.util.ColumUtil;
import ideal.sylph.plugins.hbase.util.HbaseHelper;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions.checkState;

@Name("hbase")
@Description("this is hbase Sink, if table not execit ze create table")
public class HbaseSink
        implements RealTimeSink
{
    private String tableName;
    private transient HbaseHelper hbaseHelper;
    private int rowkeyIndex = -1;
    private final Schema schema;
    private final HbaseConfig config;
    private Map<String, Tuple2<String, String>> columMapping;
    private static final Logger logger = LoggerFactory.getLogger(HbaseSink.class);

    public HbaseSink(SinkContext context, HbaseConfig config)
            throws Exception
    {
        {
            this.config = config;
            schema = context.getSchema();
            tableName = context.getSinkTable();
            if (config.nameSpace != null) {
                tableName = config.nameSpace + ":" + tableName;
            }
            hbaseHelper = new HbaseHelper(tableName, config.zookeeper, config.zkNodeParent);
            if (!hbaseHelper.tableExist(tableName)) {
                throw new TableNotFoundException("table does not exist, table name " + tableName);
            }
            columMapping = ColumUtil.mapping(schema, config.columnMapping);
            if (!Strings.isNullOrEmpty(config.rowkey)) {
                int fieldIndex = schema.getFieldIndex(config.rowkey);
                checkState(fieldIndex != -1, config.rowkey + " does not exist, only " + schema.getFields());
                this.rowkeyIndex = fieldIndex;
            }
            checkState(rowkeyIndex != -1, "`rowkey` must be set");
            hbaseHelper.closeConnection();
        }
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        if (hbaseHelper == null) {
            hbaseHelper = new HbaseHelper(tableName, config.zookeeper, config.zkNodeParent);
        }
        return true;
    }

    @Override
    public void process(Row value)
    {
        Object rowkey = value.getAs(rowkeyIndex);
        if (rowkey == null) {
            return;
        }
        Put put = new Put(BytesUtil.toBytes(rowkey));
        try {
            for (String fieldName : schema.getFieldNames()) {
                if (!config.rowkey.equals(fieldName)) {
                    Tuple2<String, String> tuple2 = columMapping.get(fieldName);
                    if (tuple2 != null) {
                        hbaseHelper.addColumn(tuple2.f0(), tuple2.f1(), value.getAs(fieldName), put);
                    }
                    else {
                        logger.warn("Field:" + fieldName + " not defined in table " + tableName);
                    }
                }
            }
            if (!put.isEmpty()) {
                hbaseHelper.store(put);
            }
        }
        catch (Exception e) {
            logger.error("put record to hbase fail.", e);
        }
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        try {
            hbaseHelper.flush();
        }
        catch (IOException e) {
            logger.error("flush records fail.", e);
        }
    }

    public static final class HbaseConfig
            extends PluginConfig
    {
        @Name("hbase.zookeeper.quorum")
        @Description("this is zookeeper hosts.")
        private String zookeeper = "master01:2181,master02:2181";

        @Name("zookeeper.znode.parent")
        @Description("this is zookeeper znode parent.")
        private String zkNodeParent;

        @Name("hbase.name.space")
        @ideal.sylph.annotation.Description("this is namespace for table.")
        private String nameSpace = "default";

        @Name("rowkey")
        @ideal.sylph.annotation.Description("this is rowkey field.")
        private String rowkey;

        @Name("column_mapping")
        @ideal.sylph.annotation.Description("this is column mapping.")
        private String columnMapping;

        public String getZookeeper()
        {
            return zookeeper;
        }

        public String getZkNodeParent()
        {
            return zkNodeParent;
        }

        public String getNameSpace()
        {
            return nameSpace;
        }

        public String getRowkey()
        {
            return rowkey;
        }

        public String getColumnMapping()
        {
            return columnMapping;
        }
    }
}
