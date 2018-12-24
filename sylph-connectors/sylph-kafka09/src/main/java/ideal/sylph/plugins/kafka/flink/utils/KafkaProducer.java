package ideal.sylph.plugins.kafka.flink.utils;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaProducer implements IProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private String brokersString;
    private String topic;
    private String partitionKey = "";
    private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;


    public KafkaProducer(String zkConnect,  String topic) {
        this.topic = topic;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy);
        client.start();

        // Get the current kafka brokers and its IDs from ZK
        List<String> ids = Collections.emptyList();
        List<String> hosts = new ArrayList<>();

        try {
            ids = client.getChildren().forPath("/brokers/ids");
        } catch (Exception ex) {
            log.error("Couldn't get brokers ids", ex);
        }

        // Get the host and port from each of the brokers
        for (String id : ids) {
            String jsonString = null;

            try {
                jsonString = new String(client.getData().forPath("/brokers/ids/" + id), "UTF-8");
            } catch (Exception ex) {
                log.error("Couldn't parse brokers data", ex);
            }

            if (jsonString != null) {
                try {
                    Gson gson = new Gson();
                    Map json = gson.fromJson(jsonString, Map.class);
                    Double port = (Double) json.get("port");
                    String host = json.get("host") + ":" + port.intValue();
                    hosts.add(host);
                } catch (NullPointerException e) {
                    log.error("Failed converting a JSON tuple to a Map class", e);
                }
            }
        }

        // Close the zookeeper connection
        client.close();

        brokersString = Joiner.on(',').join(hosts);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersString);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "60");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "500");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "ideal.sylph.plugins.kafka.flink.utils.SimplePartitioner");

        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void send(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record);
    }
}
