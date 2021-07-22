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

import ideal.sylph.TableContext;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Record;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.plugins.hdfs.factory.HDFSFactorys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

    private OutputFormat hdfsFactory;

    public HdfsSink(HdfsSinkConfig config, TableContext context)
    {
        this.config = config;
        this.sinkTable = context.getTableName();
        this.schema = context.getSchema();
        checkState(sinkTable.length() > 0, "sinkTable is " + sinkTable);

        checkState("text".equals(config.format.toLowerCase()) || "parquet".equals(config.format.toLowerCase()),
                "Hdfs sink format only supports text and parquet");
    }

    @Override
    public void process(Record value)
    {
        try {
            hdfsFactory.writeLine(value);
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
                        .partition(partitionId)
                        .config(config)
                        .getOrCreate();
                break;

            default:
                throw new UnsupportedOperationException("Hdfs sink format only supports text");
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

        @Name("file.split.size")
        @Description("default:128MB")
        private long fileSplitSize = 128L;

        @Name("batchBufferSize")
        @Description("default:5MB")
        private long batchBufferSize = 5L;

        @Name("maxCloseMinute")
        @Description("default:30 Minute")
        private long maxCloseMinute = 30;

        public long getBatchBufferSize()
        {
            return this.batchBufferSize;
        }

        public long getFileSplitSize()
        {
            return this.fileSplitSize;
        }

        public String getFormat()
        {
            return this.format;
        }

        public String getWriteDir()
        {
            return this.writeDir;
        }

        public long getMaxCloseMinute()
        {
            return maxCloseMinute;
        }
    }
}
