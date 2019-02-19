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

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.Sink;
import ideal.sylph.plugins.hdfs.utils.EventTimeBucketAssigner;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

@Name("fhdfssink")
@Description("this is hdfs sink plugin")
public class FHdfsSink
        implements Sink<DataStream<Row>> {

    private String writeDir;
    private String format;
    private final ideal.sylph.etl.Row.Schema schema;
    public FHdfsSink(SinkContext context, HdfsSinkConfig config) {
        this.writeDir=config.writeDir;
        this.format=config.format;
        this.schema = context.getSchema();
    }

    @Override
    public void run(DataStream<Row> stream){


        if(format.equals("text")){
            RollingPolicy<Row, String> rollingPolicy = DefaultRollingPolicy.create()
                    .withRolloverInterval(300000)
                    .build();
            StreamingFileSink<Row> sink = StreamingFileSink
                    .forRowFormat(new Path(writeDir), new SimpleStringEncoder<Row>())
                    .withBucketAssigner(new EventTimeBucketAssigner())
                    .withRollingPolicy(rollingPolicy)
                    .withBucketCheckInterval(300000)
                    .build();
            stream.addSink(sink);
        }else { // parquet or orcfile
            RollingPolicy<Row, String> rollingPolicy = DefaultRollingPolicy.create()
                    .withRolloverInterval(300000)
                    .build();
            StreamingFileSink<Row> sink = StreamingFileSink
                    .forRowFormat(new Path(writeDir), new SimpleStringEncoder<Row>())
                    .withBucketAssigner(new EventTimeBucketAssigner())
                    .withRollingPolicy(rollingPolicy)
                    .withBucketCheckInterval(300000)
                    .build();
            stream.addSink(sink);



//            RollingPolicy<Row, String> rollingPolicy = DefaultRollingPolicy.create()
//                    .withRolloverInterval(300000)
//                    .build();
//            StreamingFileSink<Row> sink = StreamingFileSink
//                    .forRowFormat(new Path(writeDir), ParquetBulkWriter )
//                    .withBucketAssigner(new EventTimeBucketAssigner())
//                    .withRollingPolicy(rollingPolicy)
//                    .withBucketCheckInterval(300000)
//                    .build();
//
//            stream.addSink(
//                    StreamingFileSink.forBulkFormat(
//                            Path.fromLocalFile(new File("")),
//                            ParquetAvroWriters.forGenericRecord("schema")
//                            .build()));

//            stream.addSink(sink);
        }


//        BucketingSink sink = new BucketingSink<>("hdfs:///tmp/kafka-loader2");
//        sink.setBucketer(new DateTimeBucketer<String>("yyyyMMdd"));
//        sink.setWriter(new StringWriter<>());
//        sink.setInactiveBucketCheckInterval(1L);
//        sink.setBatchSize(1024*1024*50);
//        stream.addSink(sink);
    }

    public static class HdfsSinkConfig
            extends PluginConfig
    {
        @Name("format")
        @Description("this is write file type, text or parquet")
        private String format = "text"; //text or parquet

        @Name("hdfs_write_dir")
        @Description("this is write dir")
        private String writeDir="hdfs:///tmp/sylph/dt=20190101";
    }
}
