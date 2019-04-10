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

import com.hadoop.compression.lzo.LzopCodec;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.Schema;
import ideal.sylph.plugins.hdfs.HdfsSink;
import ideal.sylph.plugins.hdfs.parquet.HDFSFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static ideal.sylph.plugins.hdfs.factory.HDFSFactorys.getRowKey;
import static java.util.Objects.requireNonNull;

/**
 * write text
 */
public class TextFileFactory
        implements HDFSFactory
{
    private static final Logger logger = LoggerFactory.getLogger(TextFileFactory.class);
    private final Map<String, FileChannel> writerManager = new HashCache();

    private final String writeTableDir;
    private final String table;

    private final long partition;
    private final int batchSize;
    private final long fileSplitSize;

    public TextFileFactory(String table, Schema schema,
            HdfsSink.HdfsSinkConfig config,
            long partition)
    {
        this.partition = partition;
        this.writeTableDir = config.getWriteDir().endsWith("/") ? config.getWriteDir() : config.getWriteDir() + "/";
        this.table = requireNonNull(table, "table is null");
        this.batchSize = (int) config.getBatchBufferSize() * 1024 * 1024;
        this.fileSplitSize = config.getFileSplitSize() * 1024L * 1024L * 8L;

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

    private FileChannel getTxtFileWriter(long eventTime)
            throws IOException
    {
        TextTimeParser timeParser = new TextTimeParser(eventTime);
        String rowKey = getRowKey(this.table, timeParser) + "\u0001" + this.partition;
        FileChannel writer = this.writerManager.get(rowKey);
        if (writer == null) {
            FileChannel fileChannel = new FileChannel(0L, this.createOutputStream(rowKey, timeParser, 0L));
            this.writerManager.put(rowKey, fileChannel);
            return fileChannel;
        }
        else if (writer.getWriteSize() > this.fileSplitSize) {
            writer.close();
            logger.info("close textFile: {}, size:{}", rowKey, writer.getWriteSize());
            long split = writer.getSplit() + 1L;
            FileChannel fileChannel = new FileChannel(split, this.createOutputStream(rowKey, timeParser, split));
            this.writerManager.put(rowKey, fileChannel);
            return fileChannel;
        }
        else {
            return writer;
        }
    }

    private OutputStream createOutputStream(String rowKey, TextTimeParser timeParser, long split)
    {
        Configuration hadoopConf = new Configuration();
        CompressionCodec codec = ReflectionUtils.newInstance(LzopCodec.class, hadoopConf);
        String outputPath = this.writeTableDir + timeParser.getPartitionPath() + "_partition_" + this.partition + "_split" + split + codec.getDefaultExtension();
        logger.info("create {} text file {}", rowKey, outputPath);

        try {
            Path path = new Path(outputPath);
            FileSystem hdfs = path.getFileSystem(hadoopConf);
            OutputStream outputStream = hdfs.exists(path) ? hdfs.append(path) : hdfs.create(path, false);
            return codec.createOutputStream(outputStream);
        }
        catch (IOException var11) {
            throw new RuntimeException("textFile " + outputPath + " writer create failed", var11);
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
        this.writeLine(eventTime, evalRow.values());
    }

    @Override
    public void writeLine(long eventTime, Collection<Object> evalRow)
            throws IOException
    {
        StringBuilder builder = new StringBuilder();
        int i = 0;
        for (Object value : evalRow) {
            if (i != 0) {
                builder.append("\u0001");
            }
            if (value != null) {
                builder.append(value.toString());
            }
            i++;
        }

        String value = builder.toString();
        this.writeLine(eventTime, value);
    }

    @Override
    public void writeLine(long eventTime, Row row)
            throws IOException
    {
        String value = row.mkString("\u0001");
        this.writeLine(eventTime, value);
    }

    private void writeLine(long eventTime, String value)
            throws IOException
    {
        TextFileFactory.FileChannel writer = this.getTxtFileWriter(eventTime);
        byte[] bytes = (value + "\n").getBytes(StandardCharsets.UTF_8);
        writer.write(bytes);
    }

    @Override
    public void close()
            throws IOException
    {
        this.writerManager.forEach((k, v) -> {
            try {
                v.close();
            }
            catch (IOException var3) {
                logger.error("close {}", k, var3);
            }
        });
    }

    private class FileChannel
    {
        private final OutputStream outputStream;

        private long writeSize = 0L;
        private long bufferSize;
        private final long split;

        public FileChannel(long split, OutputStream outputStream)
        {
            this.split = split;
            this.outputStream = outputStream;
        }

        private void write(byte[] bytes)
                throws IOException
        {
            outputStream.write(bytes);
            bufferSize += bytes.length;

            if (bufferSize > batchSize) {
                this.outputStream.flush();
                this.writeSize += this.bufferSize;
                this.bufferSize = 0L;
            }
        }

        public long getWriteSize()
        {
            return writeSize;
        }

        public long getSplit()
        {
            return split;
        }

        public void close()
                throws IOException
        {
            outputStream.close();
        }
    }

    // An LRU cache using a linked hash map
    private static class HashCache
            extends LinkedHashMap<String, FileChannel>
    {
        private static final int CACHE_SIZE = 1024;
        private static final int INIT_SIZE = 64;
        private static final float LOAD_FACTOR = 0.6f;

        HashCache()
        {
            super(INIT_SIZE, LOAD_FACTOR);
        }

        private static final long serialVersionUID = 1;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, FileChannel> eldest)
        {
            if (size() > CACHE_SIZE) {
                try {
                    eldest.getValue().close();
                    logger.info("close textFile: {}", eldest.getKey());
                    return true;
                }
                catch (IOException e) {
                    throw throwsException(e);
                }
            }
            else {
                return false;
            }
        }
    }
}
