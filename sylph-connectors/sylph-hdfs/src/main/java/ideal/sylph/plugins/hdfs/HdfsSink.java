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
package ideal.sylph.plugins.hdfs;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.plugins.hdfs.factory.HDFSFactorys;
import ideal.sylph.plugins.hdfs.parquet.HDFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

@Name("hdfs")
@Description("this is hdfs RealTimeSink")
@Version("1.0.0")
public class HdfsSink
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(HdfsSink.class);
    private final HdfsSinkConfig config;
    private final String sinkTable;
    private final Schema schema;
    private int eventTimeIndex = -1;

    private HDFSFactory hdfsFactory;

    public HdfsSink(HdfsSinkConfig config, SinkContext context)
    {
        this.config = config;
        this.sinkTable = context.getSinkTable();
        this.schema = context.getSchema();
        checkState(sinkTable.length() > 0, "sinkTable is " + sinkTable);

        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            if (schema.getFieldNames().get(i).equalsIgnoreCase(config.eventTimeName)) {
                this.eventTimeIndex = i;
                break;
            }
        }
        checkState(eventTimeIndex != -1, "eventTime_field " + config.eventTimeName + " does not exist,but only " + schema.getFieldNames());

        checkState("text".equals(config.format.toLowerCase()) || "parquet".equals(config.format.toLowerCase()),
                "Hdfs sink format only supports text and parquet");
    }

    @Override
    public void process(Row value)
    {
        try {
            long eventTime = value.getAs(eventTimeIndex);
            hdfsFactory.writeLine(eventTime, value);
        }
        catch (ClassCastException e) {
            logger.error("eventTimeField {}, index [{}], but value is {}", config.eventTimeName, eventTimeIndex, value.getAs(eventTimeIndex));
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            }
            catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        }
        catch (IOException e) {
            logger.error("", e);
        }
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        switch (config.format.toLowerCase()) {
            case "text":
                this.hdfsFactory = HDFSFactorys.getTextFileWriter()
                        .tableName(sinkTable)
                        .schema(schema)
                        .writeTableDir(config.writeDir)
                        .getOrCreate();
                break;

            case "parquet":
                this.hdfsFactory = HDFSFactorys.getParquetWriter()
                        .tableName(sinkTable)
                        .schema(schema)
                        .writeTableDir(config.writeDir)
                        .getOrCreate();
                break;
            default:
                throw new UnsupportedOperationException("Hdfs sink format only supports text and parquet");
        }

        return true;
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        try {
            hdfsFactory.close();
        }
        catch (IOException e) {
            logger.error("", e);
        }
    }

    public static class HdfsSinkConfig
            extends PluginConfig
    {
        @Name("format")
        @Description("this is write file type, text or parquet")
        private String format = "parquet";

        @Name("hdfs_write_dir")
        @Description("this is write dir")
        private String writeDir;

        @Name("eventTime_field")
        @Description("this is your data eventTime_field, 必须是13位时间戳")
        private String eventTimeName;
    }
}
