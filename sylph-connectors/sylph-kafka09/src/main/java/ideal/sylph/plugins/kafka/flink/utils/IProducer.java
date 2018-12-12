package ideal.sylph.plugins.kafka.flink.utils;

public interface IProducer {
    void send(String message);
    void close();
}