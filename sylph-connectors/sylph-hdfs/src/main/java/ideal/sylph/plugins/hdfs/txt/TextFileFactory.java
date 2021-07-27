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

import ideal.sylph.etl.Record;
import ideal.sylph.etl.Schema;
import ideal.sylph.plugins.hdfs.HdfsSink;
import ideal.sylph.plugins.hdfs.OutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static ideal.sylph.plugins.hdfs.factory.HDFSFactorys.getRowKey;
import static java.util.Objects.requireNonNull;

/**
 * write text
 */
public class TextFileFactory
        implements OutputFormat
{
    private static final Logger logger = LoggerFactory.getLogger(TextFileFactory.class);

    private final String writeTableDir;
    private final String table;

    private final long partition;
    private final int batchSize;
    private final long fileSplitSize;
    private final int maxCloseMinute;           //文件创建多久就关闭

    private FileChannel channel;

    public TextFileFactory(String table, Schema schema,
            HdfsSink.HdfsSinkConfig config,
            long partition)
    {
        this.partition = partition;
        this.writeTableDir = config.getWriteDir().endsWith("/") ? config.getWriteDir() : config.getWriteDir() + "/";
        this.table = requireNonNull(table, "table is null");
        this.batchSize = (int) config.getBatchBufferSize() * 1024 * 1024;
        this.fileSplitSize = config.getFileSplitSize() * 1024L * 1024L * 8L;
        checkState(config.getMaxCloseMinute() >= 5, "maxCloseMinute must > 5Minute");
        this.maxCloseMinute = ((int) config.getMaxCloseMinute()) * 60_000;
    }

    private FileChannel getTxtFileWriter()
            throws IOException
    {
        if (this.channel == null) {
            TextTimeParser timeParser = new TextTimeParser(System.currentTimeMillis());
            String rowKey = getRowKey(this.table, timeParser) + "\u0001" + this.partition;
            FileChannel fileChannel = this.createOutputStream(rowKey, timeParser, 0L);
            this.channel = fileChannel;
            return fileChannel;
        }
        else if (this.channel.getWriteSize() > this.fileSplitSize) {
            this.channel.close();
            logger.info("close >MaxSplitSize[{}] textFile: {}, size:{}, createTime {}",
                    fileSplitSize, this.channel.getFilePath(), this.channel.getWriteSize(), this.channel.getCreateTime());
            long split = this.channel.getSplit() + 1L;

            TextTimeParser timeParser = new TextTimeParser(System.currentTimeMillis());
            String rowKey = getRowKey(this.table, timeParser) + "\u0001" + this.partition;
            FileChannel fileChannel = this.createOutputStream(rowKey, timeParser, split);
            this.channel = fileChannel;
            return fileChannel;
        }
        else {
            return this.channel;
        }
    }

    private FileChannel createOutputStream(String rowKey, TextTimeParser timeParser, long split)
    {
        Configuration hadoopConf = new Configuration();
        CompressionCodec codec = ReflectionUtils.newInstance(Lz4Codec.class, hadoopConf);
        String outputPath = this.writeTableDir + timeParser.getPartitionPath() + "_partition_" + this.partition + "_split" + split + codec.getDefaultExtension();
        logger.info("create {} text file {}", rowKey, outputPath);
        try {
            FileChannel fileChannel = new FileChannel(outputPath, split, codec, hadoopConf);
            return fileChannel;
        }
        catch (IOException var11) {
            throw new RuntimeException("textFile " + outputPath + " writer create failed", var11);
        }
    }

    public String getWriteDir()
    {
        return writeTableDir;
    }

    public void writeLine(Record record)
            throws IOException
    {
        String value = record.mkString("\u0001");
        TextFileFactory.FileChannel writer = this.getTxtFileWriter();
        byte[] bytes = (value + "\n").getBytes(StandardCharsets.UTF_8);
        writer.write(bytes);
    }

    public void close()
            throws IOException
    {
        if (channel != null) {
            this.channel.close();
        }
    }

    private class FileChannel
    {
        private final long createTime = System.currentTimeMillis();
        private final FileSystem hdfs;
        private final String filePath;
        private final OutputStream outputStream;

        private long writeSize = 0L;
        private long bufferSize;
        private final long split;

        public FileChannel(String outputPath, long split, CompressionCodec codec, Configuration hadoopConf)
                throws IOException
        {
            Path path = new Path(outputPath);
            this.hdfs = path.getFileSystem(hadoopConf);
            OutputStream outputStream = hdfs.exists(path) ? hdfs.append(path) : hdfs.create(path, false);

            this.filePath = outputPath;
            this.split = split;
            this.outputStream = codec.createOutputStream(outputStream);
        }

        private void write(byte[] bytes)
                throws IOException
        {
            outputStream.write(bytes);
            bufferSize += bytes.length;
            this.writeSize += bytes.length;

            if (bufferSize > batchSize) {
                this.outputStream.flush();
                this.bufferSize = 0L;
            }
        }

        public String getFilePath()
        {
            return filePath;
        }

        public long getCreateTime()
        {
            return createTime;
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
            hdfs.rename(new Path(filePath), new Path(filePath.replace("_tmp_", "text_")));
        }
    }
}
