package ideal.sylph.plugins.flink.source;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.api.Source;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.calcite.shaded.com.google.common.base.Supplier;
import org.apache.flink.calcite.shaded.com.google.common.base.Suppliers;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

@Name(value = "kafka")
@Version("1.0.0")
@Description("this flink kafka source inputStream")
public class KafkaSource
        implements Source<StreamTableEnvironment, DataStream<Row>>
{
    private static final long serialVersionUID = 2L;
    private static final String[] KAFKA_COLUMNS = new String[] {"_topic", "_key", "_message", "_partition", "_offset"};

    private transient java.util.Map<String, Object> optionMap;
    private transient StreamTableEnvironment tableEnv;

    /**
     * 初始化(driver阶段执行)
     **/
    @Override
    public void driverInit(
            StreamTableEnvironment tableEnv,
            java.util.Map<String, Object> optionMap)
    {
        this.optionMap = optionMap;
        this.tableEnv = tableEnv;
    }

    private final transient Supplier<DataStream<Row>> loadStream = Suppliers.memoize(() -> {
        String topics = (String) optionMap.get("kafka_topic");
        String brokers = (String) optionMap.get("kafka_broker"); //需要把集群的host 配置到程序所在机器
        String groupid = (String) optionMap.get("kafka_group_id"); //消费者的名字
        String offset = (String) optionMap.getOrDefault("auto.offset.reset", "latest"); //latest earliest

        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        //"enable.auto.commit" -> (false: java.lang.Boolean), //不自动提交偏移量
        //      "session.timeout.ms" -> "30000", //session默认是30秒 超过5秒不提交offect就会报错
        //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
        properties.put("group.id", groupid); //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
        properties.put("auto.offset.reset", offset); //latest   earliest

        List<String> topicSets = Arrays.asList(topics.split(","));
        //org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
        DataStream<Row> stream = FlinkEnvUtil.getFlinkEnv(tableEnv).addSource(new FlinkKafkaConsumer010<Row>(
                topicSets,
                new RowDeserializer(),
                properties)
        );
        //------------registerDataStream--------------
        String tableName = (String) optionMap.getOrDefault("table_name", null);
        if (tableName != null) {
            tableEnv.registerDataStream(tableName, stream, String.join(",", KAFKA_COLUMNS) + ",proctime.proctime");
        }
        return stream;
    });

    @Override
    public DataStream<Row> getSource()
    {
        return loadStream.get();
    }

    private class RowDeserializer
            implements KeyedDeserializationSchema<Row>
    {
        @Override
        public boolean isEndOfStream(Row nextElement)
        {
            return false;
        }

        @Override
        public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
        {
            return Row.of(
                    topic, //topic
                    messageKey == null ? null : new String(messageKey, UTF_8), //key
                    new String(message, UTF_8), //message
                    partition,
                    offset
            );
        }

        public TypeInformation<Row> getProducedType()
        {
            TypeInformation<?>[] types = new TypeInformation<?>[] {
                    TypeExtractor.createTypeInfo(String.class),
                    TypeExtractor.createTypeInfo(String.class), //createTypeInformation[String]
                    TypeExtractor.createTypeInfo(String.class),
                    Types.INT,
                    Types.LONG
            };
            return new RowTypeInfo(types, KAFKA_COLUMNS);
        }
    }
}
