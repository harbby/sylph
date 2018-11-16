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
package ideal.sylph.plugins.hdfs.parquet;

import ideal.sylph.etl.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

public class ApacheParquet
        implements FileWriter
{
    private static final Logger logger = LoggerFactory.getLogger(ApacheParquet.class);

    private final ParquetWriter<Group> writer;
    private final SimpleGroupFactory groupFactory;
    private final MessageType schema;
    private final String outputPath;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock lock = rwLock.writeLock();

    private long createTime = System.currentTimeMillis();
    private long lastTime = createTime;

    private ApacheParquet(String outputPath, MessageType schema, WriterVersion writerVersion)
            throws IOException
    {
        this.schema = schema;
        this.outputPath = outputPath;

        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(schema, configuration);

        this.writer = ExampleParquetWriter.builder(new Path(outputPath))
                .withType(schema)
                .withConf(configuration)
                .withPageSize(DEFAULT_PAGE_SIZE)
                .withDictionaryPageSize(DEFAULT_PAGE_SIZE)
                .withDictionaryEncoding(DEFAULT_IS_DICTIONARY_ENABLED)
                .withValidation(DEFAULT_IS_VALIDATING_ENABLED)
                .withWriterVersion(writerVersion)
                .withRowGroupSize(DEFAULT_BLOCK_SIZE) // set Parquet file block size and page size values
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED) //压缩类型
                .build();

        this.groupFactory = new SimpleGroupFactory(this.schema);
    }

    /**
     * 获取冷落时间,即多久没有写入新数据了
     */
    @Override
    public long getCooldownTime()
    {
        return System.currentTimeMillis() - lastTime;
    }

    /**
     * 文件流已创建时间
     */
    @Override
    public long getCreatedTime()
    {
        return createTime;
    }

    @Override
    public String getWritePath()
    {
        return outputPath;
    }

    /**
     * 获取parquet流的大小
     */
    @Override
    public long getDataSize()
    {
        return writer.getDataSize();
    }

    /**
     * 入参list<Object>
     */
    @Override
    public void writeLine(List<Object> evalRow)
    {
        Group group = groupFactory.newGroup();

        List<ColumnDescriptor> columns = schema.getColumns();
        for (int i = 0; i < evalRow.size(); i++) {
            Object value = evalRow.get(i);
            addValueToGroup(columns.get(i).getType().javaType, group, i, value);
        }

        try {
            writeGroup(group);
        }
        catch (IOException e) {
            logger.error("", e);
        }
    }

    /**
     * 入参list<Object>
     */
    @Override
    public void writeLine(Row row)
    {
        Group group = groupFactory.newGroup();
        List<ColumnDescriptor> columns = schema.getColumns();
        for (int i = 0; i < row.size(); i++) {
            Object value = row.getAs(i);
            addValueToGroup(columns.get(i).getType().javaType, group, i++, value);
        }
        try {
            writeGroup(group);
        }
        catch (IOException e) {
            logger.error("", e);
        }
    }

    private Queue<String> errField = new ConcurrentLinkedQueue<>(); //每个文件的字段错误 只打印一次

    @Override
    public void writeLine(Map<String, Object> evalRow)
    {
        //--创建一个 不区分key大小写的map
        Map obj = new org.apache.commons.collections.map.CaseInsensitiveMap(evalRow);
        Group group = groupFactory.newGroup();
        int i = 0;

        for (Type field : schema.getFields()) {
            OriginalType o = field.getOriginalType();
            Class<?> javaType = (o != null && o.name().equals("MAP")) ? Map.class :
                    field.asPrimitiveType().getPrimitiveTypeName().javaType;

            Object value = obj.get(field.getName());
            try {
                addValueToGroup(javaType, group, i++, value);
            }
            catch (Exception e) {
                if (!errField.contains(field.getName())) {
                    errField.offer(field.getName());
                    logger.warn("错误字段:{}:{} 原因:{} file={}", field.getName(), value, e.getMessage(),
                            outputPath);
                }
            }
        }
        try {
            writeGroup(group);
        }
        catch (Exception e) {
            logger.warn("错误行:{} err:", evalRow, e);
        }
    }

    /**
     * 写入 一条数据
     */
    private void writeGroup(Group group)
            throws IOException
    {
        if (group == null) {
            return;
        }
        try {
            lock.lock();  //加锁
            lastTime = System.currentTimeMillis();
            writer.write(group);
        }
        finally {
            lock.unlock(); //解锁
        }
    }

    /**
     * 注意 只有关闭后 数据才会正在落地
     */
    @Override
    public void close()
            throws IOException
    {
        try {
            lock.lock();
            writer.close();
            //1,修改文件名称
            FileSystem hdfs = FileSystem.get(java.net.URI.create(outputPath), new Configuration());
            hdfs.rename(new Path(outputPath),
                    new Path(outputPath.replace("_tmp_", "file_") + ".parquet"));
            //这里注意 千万不要关闭 hdfs 否则写parquet都会出错
        }
        catch (IOException e) {
            logger.error("关闭Parquet输出流异常", e);
            FileSystem hdfs = FileSystem.get(java.net.URI.create(outputPath), new Configuration());
            hdfs.rename(new Path(outputPath), new Path(outputPath + ".err"));
        }
        finally {
            lock.unlock();
        }
    }

    /*
     * 字段类型为map时对应的map和man entry的schema
     */
    private static MessageType mapTopSchema = MessageTypeParser.parseMessageType("message row {\n" +
            "  repeated group key_value {\n" +
            "    required binary key (UTF8);\n" +
            "    optional binary value (UTF8);\n" +
            "  }\n" +
            "}\n");
    private static MessageType kvSchema = MessageTypeParser.parseMessageType("message row {\n" +
            "  required binary key (UTF8);\n" +
            "  optional binary value (UTF8);\n" +
            "}\n");

    private void addValueToGroup(Class<?> dataType, Group group, int index, Object value)
    {
        if (value == null || "".equals(value)) {
            return;
        }
        if (dataType == Binary.class) {
            group.add(index, value.toString());
        }
        else if (dataType == byte.class) {
            group.add(index, Byte.valueOf(value.toString()));
        }
        else if (dataType == short.class) {
            group.add(index, Short.valueOf(value.toString()));
        }
        else if (dataType == int.class) {
            group.add(index, Integer.valueOf(value.toString()));
        }
        else if (dataType == long.class) {
            group.add(index, Long.parseLong(value.toString()));
        }
        else if (dataType == double.class) {
            group.add(index, Double.valueOf(value.toString()));
        }
        else if (dataType == float.class) {
            group.add(index, Float.valueOf(value.toString()));
        }
        else if (dataType == Map.class) {
            int mapFieldSize = 0;
            //List<MessageType> mapSchemaList = mapEntrySchema.get(index);
            Group mapFieldGroup = new SimpleGroup(mapTopSchema);
            for (Map.Entry<String, Object> mapFieldEntry : ((Map<String, Object>) value)
                    .entrySet()) {
                Group mapEntryKeyValueGroup = new SimpleGroup(kvSchema);
                final String key = mapFieldEntry.getKey();
                final Object vValue = mapFieldEntry.getValue();
                if (vValue != null) {
                    mapEntryKeyValueGroup.add("key", key);
                    mapFieldSize += key.length();
                    mapEntryKeyValueGroup.add("value", vValue.toString());
                    mapFieldSize += vValue.toString().length();
                    mapFieldGroup.add("key_value", mapEntryKeyValueGroup);
                }
            }
            group.add(index, mapFieldGroup);
        }
        else {
            group.add(index, value.toString());
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ParquetProperties.WriterVersion parquetVersion = ParquetProperties.WriterVersion.PARQUET_2_0;
        private String writePath;
        private MessageType schema;

        public Builder schema(MessageType messageType)
        {
            this.schema = messageType;
            return this;
        }

        public Builder parquetVersion(ParquetProperties.WriterVersion parquetVersion)
        {
            this.parquetVersion = parquetVersion;
            return this;
        }

        public Builder writePath(String writePath)
        {
            this.writePath = writePath;
            return this;
        }

        public ApacheParquet build()
                throws IOException
        {
            return new ApacheParquet(writePath, schema, parquetVersion);
        }
    }
}
