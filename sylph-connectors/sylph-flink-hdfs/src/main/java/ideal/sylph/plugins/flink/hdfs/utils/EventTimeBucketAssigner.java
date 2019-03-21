package ideal.sylph.plugins.flink.hdfs.utils;


import ideal.sylph.etl.Row;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

//import org.apache.flink.types.Row;

public class EventTimeBucketAssigner implements BucketAssigner<Row, String> {
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String getBucketId(Row element, Context context) {
        String partitionValue;
        try {
            partitionValue = getPartitionValue();
        } catch (Exception e) {
            partitionValue = "1970010100";
        }
        return "dt="+partitionValue;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    private String getPartitionValue() throws Exception {
        return new SimpleDateFormat("yyyyMMddhh").format(new Date());
    }
}
