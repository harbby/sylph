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
package ideal.sylph.plugins.hdfs.txt;

import ideal.sylph.etl.Row;
import ideal.sylph.plugins.hdfs.parquet.HDFSFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static ideal.sylph.plugins.hdfs.factory.HDFSFactorys.getRowKey;
import static java.util.Objects.requireNonNull;

/**
 * 可用性 需进一步开发
 */
@Deprecated
public class TextFileFactory
        implements HDFSFactory
{
    private final Logger logger = LoggerFactory.getLogger(TextFileFactory.class);
    private final Map<String, FSDataOutputStream> writerManager = new HashMap<>();

    private final String writeTableDir;
    private final String table;
    private final Row.Schema schema;

    public TextFileFactory(
            final String writeTableDir,
            final String table,
            final Row.Schema schema)
    {
        requireNonNull(writeTableDir, "writeTableDir is null");
        this.writeTableDir = writeTableDir.endsWith("/") ? writeTableDir : writeTableDir + "/";

        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            writerManager.entrySet().stream().parallel().forEach(x -> {
                String rowKey = x.getKey();
                try {
                    x.getValue().close();
                }
                catch (IOException e) {
                    logger.error("addShutdownHook close textFile Writer failed {}", rowKey, e);
                }
            });
        }));
    }

    private FSDataOutputStream getTxtFileWriter(long eventTime)
    {
        TextTimeParser timeParser = new TextTimeParser(eventTime);
        String rowKey = getRowKey(table, timeParser);

        return getTxtFileWriter(rowKey, () -> {
            try {
                FileSystem hdfs = FileSystem.get(new Configuration());
                //CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, hdfs.getConf());
                String outputPath = writeTableDir + timeParser.getPartionPath();
                logger.info("create text file {}", outputPath);
                Path path = new Path(outputPath);
                FSDataOutputStream outputStream = hdfs.exists(path) ? hdfs.append(path) : hdfs.create(path, false);
                //return codec.createOutputStream(outputStream);
                return outputStream;
            }
            catch (IOException e) {
                throw new RuntimeException("textFile writer create failed", e);
            }
        });
    }

    private FSDataOutputStream getTxtFileWriter(String rowKey, Supplier<FSDataOutputStream> builder)
    {
        //2,检查流是否存在 不存在就新建立一个
        FSDataOutputStream writer = writerManager.get(rowKey);
        if (writer != null) {
            return writer;
        }
        else {
            synchronized (writerManager) {
                return writerManager.computeIfAbsent(rowKey, (key) -> builder.get());
            }
        }
    }

    @Override
    public String getWriteDir()
    {
        return writeTableDir;
    }

    @Override
    public void writeLine(long eventTime, Map<String, Object> evalRow)
            throws IOException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public void writeLine(long eventTime, List<Object> evalRow)
            throws IOException
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < evalRow.size(); i++) {
            Object value = evalRow.get(i);
            if (i != 0) {
                builder.append("\u0001");
            }
            if (value != null) {
                builder.append(value.toString());
            }
        }
        FSDataOutputStream outputStream = getTxtFileWriter(eventTime);
        writeString(outputStream, builder.toString());
    }

    @Override
    public void writeLine(long eventTime, Row row)
            throws IOException
    {
        FSDataOutputStream outputStream = getTxtFileWriter(eventTime);
        writeString(outputStream, row.mkString("\u0001"));
    }

    /**
     * todo: 存在线程安全问题
     */
    private static void writeString(FSDataOutputStream outputStream, String string)
            throws IOException
    {
        byte[] bytes = (string + "\n").getBytes(StandardCharsets.UTF_8); //先写入换行符
        outputStream.write(bytes); //经过测试 似乎是线程安全的
        int batchSize = 1024; //1k = 1024*1
        if (outputStream.size() % batchSize == 0) {
            outputStream.hsync();
            //outputStream.hflush();
        }
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
