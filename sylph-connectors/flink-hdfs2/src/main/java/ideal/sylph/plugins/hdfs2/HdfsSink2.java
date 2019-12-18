package ideal.sylph.plugins.hdfs2;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.api.Sink;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
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
                codecClass = DefaultCodec.class;
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
                builder.append(",").append(row.getField(i));
            }
            return builder.substring(1).getBytes(UTF_8);
        })
                .addSink(sink)
                .name(this.getClass().getSimpleName());
    }

    public static class LocalShuffle<IN>
            extends RichSinkFunction<IN>
            implements CheckpointedFunction
            , CheckpointListener, ProcessingTimeCallback
    {
        private final List<RichSinkFunction<IN>> sinks;
        private final BlockingQueue<Tuple2<IN, Context>> queue = new LinkedBlockingDeque<>(10_000);
        private ExecutorService service;
        private volatile boolean running = true;

        public LocalShuffle(int split, RichSinkFunction<IN> userSink)
                throws IOException, ClassNotFoundException, IllegalAccessException, NoSuchFieldException
        {
            this.sinks = new ArrayList<>(split);
            SerializedValue<RichSinkFunction<IN>> serializedValue = new SerializedValue<>(userSink);
            for (int i = 0; i < split; i++) {
                StreamingFileSink<IN> sink = (StreamingFileSink<IN>) serializedValue.deserializeValue(this.getClass().getClassLoader());
                Field field = StreamingFileSink.class.getDeclaredField("bucketsBuilder");
                field.setAccessible(true);
                StreamingFileSink<IN> mockSink = new StreamingFileSink<IN>((StreamingFileSink.BulkFormatBuilder<IN, ?>) field.get(sink), 0)
                {
                    @Override
                    public RuntimeContext getRuntimeContext()
                    {
                        return LocalShuffle.this.getRuntimeContext();
                    }
                };
            }
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters)
                throws Exception
        {
            super.open(parameters);
            this.service = Executors.newFixedThreadPool(sinks.size());
            for (RichSinkFunction<IN> sink : sinks) {
                sink.open(parameters);
                service.submit(() -> {
                    while (running) {
                        try {
                            Tuple2<IN, Context> tuple2 = queue.take();
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
            for (RichSinkFunction<IN> sink : sinks) {
                sink.close();
            }
        }

        @Override
        public void invoke(IN value, Context context)
                throws Exception
        {
            queue.put(Tuple2.of(value, context));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId)
                throws Exception
        {
            for (RichSinkFunction<IN> sink : sinks) {
                if (sink instanceof CheckpointListener) {
                    ((CheckpointListener) sink).notifyCheckpointComplete(checkpointId);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context)
                throws Exception
        {
            for (RichSinkFunction<IN> sink : sinks) {
                if (sink instanceof CheckpointedFunction) {
                    ((CheckpointedFunction) sink).snapshotState(context);
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context)
                throws Exception
        {
            for (RichSinkFunction<IN> sink : sinks) {
                if (sink instanceof CheckpointedFunction) {
                    ((CheckpointedFunction) sink).initializeState(context);
                }
            }
        }

        @Override
        public void onProcessingTime(long timestamp)
                throws Exception
        {
            for (RichSinkFunction<IN> sink : sinks) {
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

        @Name("maxCloseMinute")
        @Description("default:30 Minute")
        private long maxCloseMinute = 30;

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

        public long getMaxCloseMinute()
        {
            return maxCloseMinute;
        }
    }
}
