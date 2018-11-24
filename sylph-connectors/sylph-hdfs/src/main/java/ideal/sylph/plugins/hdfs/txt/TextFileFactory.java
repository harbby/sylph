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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static ideal.sylph.plugins.hdfs.factory.HDFSFactorys.getRowKey;
import static java.util.Objects.requireNonNull;

/**
 * write text
 */
public class TextFileFactory
        implements HDFSFactory
{
    private static final Logger logger = LoggerFactory.getLogger(TextFileFactory.class);
    private final Map<String, FSDataOutputStream> writerManager = new HashCache();
    private final BlockingQueue<Tuple2<String, Long>> streamData = new LinkedBlockingQueue<>(1000);
    private final ExecutorService executorPool = Executors.newSingleThreadExecutor();

    private final String writeTableDir;
    private final String table;
    private final Row.Schema schema;

    private volatile boolean closed = false;

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

        executorPool.submit(() -> {
            Thread.currentThread().setName("Text_Factory_Consumer");
            while (!closed) {
                Tuple2<String, Long> tuple2 = streamData.poll();
                if (tuple2 != null) {
                    long eventTime = tuple2.f2();
                    String value = tuple2.f1();
                    FSDataOutputStream outputStream = getTxtFileWriter(eventTime);
                    writeString(outputStream, value);
                }
                else {
                    TimeUnit.MILLISECONDS.sleep(1);
                }
            }
            return null;
        });
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
        try {
            streamData.put(Tuple2.of(builder.toString(), eventTime));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void writeLine(long eventTime, Row row)
            throws IOException
    {
        try {
            streamData.put(Tuple2.of(row.mkString("\u0001"), eventTime));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void writeString(FSDataOutputStream outputStream, String string)
            throws IOException
    {
        byte[] bytes = (string + "\n").getBytes(StandardCharsets.UTF_8); //先写入换行符
        outputStream.write(bytes); //经过测试 似乎是线程安全的
        int batchSize = 1024; //1k = 1024*1
        if (outputStream.size() % batchSize == 0) {
            outputStream.flush();
        }
    }

    @Override
    public void close()
            throws IOException
    {
    }

    public static class Tuple2<T1, T2>
    {
        private final T1 t1;
        private final T2 t2;

        public Tuple2(T1 t1, T2 t2)
        {
            this.t1 = t1;
            this.t2 = t2;
        }

        public static <T1, T2> Tuple2<T1, T2> of(T1 t1, T2 t2)
        {
            return new Tuple2<>(t1, t2);
        }

        public T1 f1()
        {
            return t1;
        }

        public T2 f2()
        {
            return t2;
        }
    }

    // An LRU cache using a linked hash map
    private static class HashCache
            extends LinkedHashMap<String, FSDataOutputStream>
    {
        private static final int CACHE_SIZE = 64;
        private static final int INIT_SIZE = 32;
        private static final float LOAD_FACTOR = 0.6f;

        HashCache()
        {
            super(INIT_SIZE, LOAD_FACTOR);
        }

        private static final long serialVersionUID = 1;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, FSDataOutputStream> eldest)
        {
            if (size() > CACHE_SIZE) {
                try {
                    eldest.getValue().close();
                    logger.info("close textFile: {}", eldest.getKey());
                    return true;
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                return false;
            }
        }
    }
}
