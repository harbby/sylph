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
package ideal.sylph.plugins.hdfs.factory;

import ideal.sylph.etl.Schema;
import ideal.sylph.plugins.hdfs.HdfsSink;
import ideal.sylph.plugins.hdfs.OutputFormat;
import ideal.sylph.plugins.hdfs.txt.TextFileFactory;

import static java.util.Objects.requireNonNull;

public class HDFSFactorys
{
    private HDFSFactorys() {}

    public static Builder getTextFileWriter()
    {
        return new TextFileWriterBuilder();
    }

    public static class TextFileWriterBuilder
            extends Builder
    {
        @Override
        public OutputFormat getOrCreate()
        {
            requireNonNull(schema, "schema is null");
            requireNonNull(tableName, "必须传入tableName,如表 xxx_log");
            requireNonNull(sinkConfig.getWriteDir(), "必须传入writeTableDir,如: hdfs:///tmp/hive/xxx_log");

            return new TextFileFactory(tableName, schema, sinkConfig, partition);
        }
    }

    public abstract static class Builder
    {
        protected String tableName;
        protected Schema schema;
        protected HdfsSink.HdfsSinkConfig sinkConfig;
        protected long partition;
        protected String writeTableDir;

        /**
         * 注意在两级key 这个是用来区分不同的表的 仅此而已
         * rowkey = table + partition_key
         */
        public Builder tableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder writeTableDir(String writeTableDir)
        {
            this.writeTableDir = writeTableDir;
            return this;
        }

        public Builder partition(long partition)
        {
            this.partition = partition;
            return this;
        }

        public Builder config(HdfsSink.HdfsSinkConfig sinkConfig)
        {
            this.sinkConfig = sinkConfig;
            return this;
        }

        public Builder schema(Schema schema)
        {
            this.schema = schema;
            return this;
        }

        public abstract OutputFormat getOrCreate();
    }

    public static String getRowKey(String table, TimeParser timeParser)
    {
        return table + "\u0001" + timeParser.getWriterKey();
    }
}
