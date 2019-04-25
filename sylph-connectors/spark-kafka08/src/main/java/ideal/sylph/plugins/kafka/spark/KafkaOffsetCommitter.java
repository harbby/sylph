package ideal.sylph.plugins.kafka.spark;

import kafka.common.TopicAndPartition;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map$;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static org.spark_project.guava.base.Preconditions.checkArgument;

public class KafkaOffsetCommitter
        extends Thread
        implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetCommitter.class);

    private final KafkaCluster kafkaCluster;
    private final String groupId;

    /**
     * Flag to mark the periodic committer as running.
     */
    private volatile boolean running = true;

    private final int commitInterval;
    private final Queue<OffsetRange> commitQueue;

    public KafkaOffsetCommitter(
            KafkaCluster kafkaCluster,
            String groupId,
            int commitInterval,
            Queue<OffsetRange> commitQueue)
    {
        checkArgument(commitInterval >= 5000, "commitInterval must >= 5000");
        this.commitInterval = commitInterval;
        this.kafkaCluster = kafkaCluster;
        this.groupId = groupId;
        this.commitQueue = commitQueue;
    }

    @Override
    public void close()
    {
        running = false;
    }

    @Override
    public void run()
    {
        while (running) {
            try {
                Thread.sleep(commitInterval);
                commitAll();
            }
            catch (Throwable t) {
                logger.error("The offset committer encountered an error: {}", t.getMessage(), t);
            }
        }
    }

    private void commitAll()
            throws Exception
    {
        Map<TopicAndPartition, Long> m = new HashMap<TopicAndPartition, Long>();
        OffsetRange osr = commitQueue.poll();
        while (null != osr) {
            TopicAndPartition tp = osr.topicAndPartition();
            Long x = m.get(tp);
            long offset = (null == x) ? osr.untilOffset() : Math.max(x, osr.untilOffset());
            m.put(tp, offset);
            osr = commitQueue.poll();
        }
        if (!m.isEmpty()) {
            commitKafkaOffsets(m);
            //consumer.commitAsync(m, commitCallback.get)
        }
    }

    @SuppressWarnings("unchecked")
    private void commitKafkaOffsets(Map<TopicAndPartition, Long> internalOffsets)
            throws Exception
    {
        logger.info("committing offset to kafka, {}", internalOffsets);

        Seq<Tuple2<TopicAndPartition, Long>> fromOffsetsAsJava = JavaConverters.mapAsScalaMapConverter(internalOffsets).asScala().toSeq();
        kafkaCluster.setConsumerOffsets(groupId, (scala.collection.immutable.Map<TopicAndPartition, Object>) Map$.MODULE$.<TopicAndPartition, Long>apply(fromOffsetsAsJava));
    }
}
