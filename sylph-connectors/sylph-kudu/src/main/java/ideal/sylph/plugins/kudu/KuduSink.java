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
package ideal.sylph.plugins.kudu;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.function.Supplier;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static java.util.Objects.requireNonNull;

@Name("kudu")
@Description("this sylph kudu sink")
public class KuduSink
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(KuduSink.class);
    private final String tableName;
    private final String kuduHost;
    private final List<String> fieldNames;
    private final KuduSinkConfig kuduSinkConfig;

    private KuduClient kuduClient;
    private KuduSession kuduSession;
    private KuduTable kuduTable;

    private final int maxBatchSize;
    private final int mutationBufferSpace;

    private int rowNumCnt = 0;

    private Supplier<Operation> operationCreater;

    public KuduSink(SinkContext context, KuduSinkConfig kuduSinkConfig)
    {
        this.kuduSinkConfig = kuduSinkConfig;
        this.tableName = requireNonNull(kuduSinkConfig.tableName, "kudu.table is null");
        this.kuduHost = requireNonNull(kuduSinkConfig.hosts, "kudu.hosts is null");
        this.fieldNames = context.getSchema().getFieldNames();

        this.maxBatchSize = (int) kuduSinkConfig.batchSize;
        this.mutationBufferSpace = (int) kuduSinkConfig.mutationBufferSpace;

        //--check write mode
        getOperationCreater(kuduSinkConfig.mode, null);
        logger.info("kudu config: {}", kuduSinkConfig);
    }

    private static Supplier<Operation> getOperationCreater(String mode, KuduTable kuduTable)
    {
        //INSERT OR UPSET OR UPDATE OR DELETE
        switch (mode.toUpperCase()) {
            case "INSERT":
                return () -> kuduTable.newInsert();
            case "UPSET":
                return () -> kuduTable.newUpsert();
            case "UPDATE":
                return () -> kuduTable.newUpdate();
            case "DELETE":
                return () -> kuduTable.newDelete();
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        this.kuduClient = new KuduClient.KuduClientBuilder(kuduHost).build();
        this.kuduSession = kuduClient.newSession();
        this.kuduTable = kuduClient.openTable(tableName);
        this.operationCreater = getOperationCreater(kuduSinkConfig.mode, kuduTable);

        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        //kuduSession.setFlushInterval();
        this.kuduSession.setMutationBufferSpace(this.mutationBufferSpace); //8m
        return true;
    }

    @Override
    public void process(Row row)
    {
        Operation operation = operationCreater.get();
        try {
            for (int i = 0; i < fieldNames.size(); i++) {
                appendColumn(operation, fieldNames.get(i), row.getField(i));
            }

            kuduSession.apply(operation);
            // submit batch
            if (rowNumCnt++ > maxBatchSize) {
                rowNumCnt = 0;
                kuduSession.flush(); //真正落地
            }
        }
        catch (IOException e) {
            throwsException(e);
        }
    }

    private void appendColumn(Operation operation, String name, Object value)
    {
        ColumnSchema columnSchema = kuduTable.getSchema().getColumn(name);

        if (value == null) {
            operation.getRow().setNull(name);
            return;
        }

        Type kuduType = columnSchema.getType();
        switch (kuduType) {
            case BINARY:
                operation.getRow().addBinary(name, (byte[]) value);
                break;

            case STRING:
                operation.getRow().addString(name, String.valueOf(value));
                break;
            case BOOL:
                operation.getRow().addBoolean(name, (Boolean) value);
                break;

            case INT8:
            case INT16:
                operation.getRow().addShort(name, (Short) value);
                break;

            case INT32:
                operation.getRow().addInt(name, (Integer) value);
                break;

            case INT64: {
                if (value instanceof Date) {
                    operation.getRow().addLong(name, ((Date) value).getTime());
                }
                else if (value instanceof Time) {
                    operation.getRow().addLong(name, ((Time) value).getTime());
                }
                else if (value instanceof Timestamp) {
                    operation.getRow().addLong(name, ((Timestamp) value).getTime());
                }
                else {
                    operation.getRow().addLong(name, (Long) value);
                }
                break;
            }
            case DOUBLE:
                operation.getRow().addDouble(name, (Double) value);
                break;
            case FLOAT:
                operation.getRow().addFloat(name, (Float) value);
                break;

            case DECIMAL:
                operation.getRow().addDecimal(name, (BigDecimal) value);
                break;

            default:
                throw new IllegalStateException("不受支持的kudu类型:" + kuduType);
        }
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        try (KuduClient client = kuduClient) {
            if (kuduSession != null) {
                kuduSession.close();
            }
        }
        catch (IOException e) {
            throwsException(e);
        }
    }

    public static class KuduSinkConfig
            extends PluginConfig
    {
        @Name("kudu.hosts")
        @Description("this is kudu cluster hosts, demo: slave01:7051,slave02:7051")
        private String hosts;

        @Name("kudu.tableName")
        @Description("this is kudu tableName")
        private String tableName;

        @Name("kudu.mode")
        @Description("this is kudu, INSERT OR UPSET OR UPDATE OR DELETE")
        private String mode = "UPSET";

        @Name("batchSize")
        @Description("this is kudu write lines batchSize")
        private int batchSize = 1000;

        @Name("mutationBufferSpace")
        @Description("kuduSession.setMutationBufferSpace(?)")
        private int mutationBufferSpace = 1024 * 1024 * 8;
    }
}
