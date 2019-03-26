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
import ideal.sylph.plugins.hdfs.parquet.HDFSFactory;
import ideal.sylph.plugins.hdfs.parquet.ParquetFactory;
import ideal.sylph.plugins.hdfs.txt.TextFileFactory;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;
import java.util.Map;

import static ideal.sylph.plugins.hdfs.utils.ParquetUtil.buildSchema;
import static java.util.Objects.requireNonNull;

public class HDFSFactorys
{
    private HDFSFactorys() {}

    private static final Map<Class<? extends HDFSFactory>, HDFSFactory> hdfsFactory = new HashMap<>();

    public static ParquetWriterBuilder getParquetWriter()
    {
        return new ParquetWriterBuilder();
    }

    public static Builder getTextFileWriter()
    {
        return new TextFileWriterBuilder();
    }

    public static class TextFileWriterBuilder
            extends Builder
    {
        @Override
        public HDFSFactory getOrCreate()
        {
            requireNonNull(schema, "schema is null");
            requireNonNull(tableName, "必须传入tableName,如表 xxx_log");
            requireNonNull(writeTableDir, "必须传入writeTableDir,如: hdfs:///tmp/hive/xxx_log");

            HDFSFactory factory = hdfsFactory.get(TextFileFactory.class);
            if (factory != null) {
                return factory;
            }
            else {
                synchronized (hdfsFactory) {
                    return hdfsFactory.computeIfAbsent(
                            ParquetFactory.class,
                            (k) -> new TextFileFactory(writeTableDir, tableName, schema));
                }
            }
        }
    }

    public abstract static class Builder
    {
        protected String tableName;
        protected String writeTableDir;
        protected Schema schema;

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

        public Builder schema(Schema schema)
        {
            this.schema = schema;
            return this;
        }

        public abstract HDFSFactory getOrCreate();
    }

    public static class ParquetWriterBuilder
            extends Builder
    {
        private ParquetProperties.WriterVersion parquetVersion = ParquetProperties.WriterVersion.PARQUET_2_0;

        public ParquetWriterBuilder parquetVersion(ParquetProperties.WriterVersion parquetVersion)
        {
            this.parquetVersion = parquetVersion;
            return this;
        }

        @Override
        public HDFSFactory getOrCreate()
        {
            requireNonNull(schema, "schema is null");
            requireNonNull(tableName, "必须传入tableName,如表 xxx_log");
            requireNonNull(writeTableDir, "必须传入writeTableDir,如: hdfs:///tmp/hive/xxx_log");

            HDFSFactory factory = hdfsFactory.get(ParquetFactory.class);
            if (factory != null) {
                return factory;
            }
            else {
                String schemaString = buildSchema(schema.getFields());
                MessageType type = MessageTypeParser.parseMessageType(schemaString);
                synchronized (hdfsFactory) {
                    return hdfsFactory.computeIfAbsent(
                            ParquetFactory.class,
                            (k) -> new ParquetFactory(writeTableDir, tableName, parquetVersion, type));
                }
            }
        }
    }

    public static String getRowKey(String table, TimeParser timeParser)
    {
        return table + "\u0001" + timeParser.getWriterKey();
    }
}
