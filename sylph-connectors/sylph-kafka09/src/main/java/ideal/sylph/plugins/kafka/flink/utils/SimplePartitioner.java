package ideal.sylph.plugins.kafka.flink.utils;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class SimplePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(key != null) {
            String stringKey = key.toString();
            int offset = stringKey.hashCode();
            return Math.abs(offset % cluster.partitionCountForTopic(topic));
        } else {
            return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
