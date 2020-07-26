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
package ideal.sylph.plugins.hdfs2;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.api.Sink;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static java.nio.charset.StandardCharsets.UTF_8;

@Name("hdfs")
@Description("this is hdfs RealTimeSink")
@Version("1.0.0")
public class HdfsSink2
        implements Sink<DataStream<Row>>
{
    private final long batchSize;
    private final Class<? extends CompressionCodec> codecClass;
    private final String writerDir;

    public HdfsSink2(Hdfs2SinkConfig config)
            throws ClassNotFoundException
    {
        this.batchSize = config.getBatchBufferSize();
        this.writerDir = config.getWriteDir();
        switch (config.getZipType().trim().toLowerCase()) {
            case "lzo":
                codecClass = (Class<? extends CompressionCodec>) Class.forName("com.hadoop.compression.lzo.LzopCodec");
                break;
            case "lz4":
                codecClass = Lz4Codec.class;
                break;
            case "snappy":
                codecClass = SnappyCodec.class;
                break;
            case "gzip":
                codecClass = GzipCodec.class;
                break;
            case "bzip2":
                codecClass = BZip2Codec.class;
                break;
            case "default":
                codecClass = DefaultCodec.class;  //zlib
                break;
            default:
                codecClass = NoneCodec.class;
        }
    }

    @Override
    public void run(DataStream<Row> stream)
    {
        final RichSinkFunction<byte[]> sink = StreamingFileSink.forBulkFormat(
                new Path(writerDir),
                (BulkWriter.Factory<byte[]>) fsDataOutputStream -> new BulkWriter<byte[]>()
                {
                    private final CompressionCodec codec = ReflectionUtils.newInstance(codecClass, new Configuration());
                    private final CompressionOutputStream outputStream = codec.createOutputStream(fsDataOutputStream);
                    private long bufferSize;

                    @Override
                    public void addElement(byte[] element)
                            throws IOException
                    {
                        outputStream.write(element);
                        outputStream.write(10); //write \n
                        bufferSize += element.length;
                        if (bufferSize >= batchSize) {
                            outputStream.flush();
                            this.bufferSize = 0;
                        }
                    }

                    @Override
                    public void flush()
                            throws IOException
                    {
                        outputStream.flush();
                    }

                    @Override
                    public void finish()
                            throws IOException
                    {
                        outputStream.finish();
                        outputStream.close();
                    }
                })
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
                .build();
        stream.map(row -> {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < row.getArity(); i++) {
                builder.append("\u0001").append(row.getField(i));
            }
            return builder.substring(1).getBytes(UTF_8);
        })
                .addSink(sink)
                .name(this.getClass().getSimpleName());
    }

    @VisibleForTesting
    public static class LocalShuffle<T>
            extends RichSinkFunction<T>
            implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback
    {
        private final List<RichSinkFunction<T>> sinks;
        private final BlockingQueue<Tuple2<T, Context>> queue = new LinkedBlockingDeque<>(10_000);
        private ExecutorService service;
        private volatile boolean running = true;

        public LocalShuffle(int split, RichSinkFunction<T> userSink)
                throws IOException, ClassNotFoundException, IllegalAccessException, NoSuchFieldException
        {
            this.sinks = new ArrayList<>(split);
            SerializedValue<RichSinkFunction<T>> serializedValue = new SerializedValue<>(userSink);
            for (int i = 0; i < split; i++) {
                StreamingFileSink<T> sink = (StreamingFileSink<T>) serializedValue.deserializeValue(this.getClass().getClassLoader());
                Field field = StreamingFileSink.class.getDeclaredField("bucketsBuilder");
                field.setAccessible(true);
//                StreamingFileSink<T> mockSink = new StreamingFileSink<T>((StreamingFileSink.BulkFormatBuilder) field.get(sink), 0) {
//                    @Override
//                    public RuntimeContext getRuntimeContext()
//                    {
//                        return LocalShuffle.this.getRuntimeContext();
//                    }
//                };
//                sinks.add(mockSink);
            }
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters)
                throws Exception
        {
            super.open(parameters);
            this.service = Executors.newFixedThreadPool(sinks.size());
            for (RichSinkFunction<T> sink : sinks) {
                sink.open(parameters);
                service.submit(() -> {
                    while (running) {
                        try {
                            Tuple2<T, Context> tuple2 = queue.take();
                            sink.invoke(tuple2.f0, tuple2.f1);
                        }
                        catch (InterruptedException e) {
                            break;
                        }
                        catch (Exception e) {
                            //todo; exit taskmanager
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        }

        @Override
        public void close()
                throws Exception
        {
            running = false;
            super.close();
            service.shutdown();
            for (RichSinkFunction<T> sink : sinks) {
                sink.close();
            }
        }

        @Override
        public void invoke(T value, Context context)
                throws Exception
        {
            queue.put(Tuple2.of(value, context));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId)
                throws Exception
        {
            for (RichSinkFunction<T> sink : sinks) {
                if (sink instanceof CheckpointListener) {
                    ((CheckpointListener) sink).notifyCheckpointComplete(checkpointId);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context)
                throws Exception
        {
            for (RichSinkFunction<T> sink : sinks) {
                if (sink instanceof CheckpointedFunction) {
                    ((CheckpointedFunction) sink).snapshotState(context);
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context)
                throws Exception
        {
            for (RichSinkFunction<T> sink : sinks) {
                if (sink instanceof CheckpointedFunction) {
                    ((CheckpointedFunction) sink).initializeState(context);
                }
            }
        }

        @Override
        public void onProcessingTime(long timestamp)
                throws Exception
        {
            for (RichSinkFunction<T> sink : sinks) {
                if (sink instanceof ProcessingTimeCallback) {
                    ((ProcessingTimeCallback) sink).onProcessingTime(timestamp);
                }
            }
        }
    }

    public static class Hdfs2SinkConfig
            extends PluginConfig
    {
        @Name("hdfs_write_dir")
        @Description("this is write dir")
        private String writeDir;

        @Name("zip_type")
        @Description("this is your data eventTime_field, 必须是13位时间戳")
        private String zipType = "none";

        @Name("batchBufferSize")
        @Description("default:5MB")
        private long batchBufferSize = 5 * 1024 * 1024;

        public long getBatchBufferSize()
        {
            return this.batchBufferSize;
        }

        public String getZipType()
        {
            return zipType;
        }

        public String getWriteDir()
        {
            return this.writeDir;
        }
    }
}
