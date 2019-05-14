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
package ideal.sylph.plugins.kafka.spark;

import com.github.harbby.spark.sql.kafka.model.KafkaPartitionOffset;
import kafka.common.TopicAndPartition;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.jetty.util.ConcurrentArrayQueue;
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
    private volatile boolean running = false;

    private final int commitInterval;
    private final Queue<KafkaPartitionOffset> commitQueue = new ConcurrentArrayQueue<>(1024);

    public KafkaOffsetCommitter(
            KafkaCluster kafkaCluster,
            String groupId,
            int commitInterval)
    {
        checkArgument(commitInterval >= 5000, "commitInterval must >= 5000");
        this.commitInterval = commitInterval;
        this.kafkaCluster = kafkaCluster;
        this.groupId = groupId;
    }

    @Override
    public synchronized void start()
    {
        this.setDaemon(true);
        super.start();
        running = true;
    }

    public void addAll(OffsetRange[] offsetRanges)
    {
        if (running) {
            for (OffsetRange offsetRange : offsetRanges) {
                KafkaPartitionOffset kafkaPartitionOffset = new KafkaPartitionOffset(offsetRange.topicAndPartition(), offsetRange.untilOffset());
                commitQueue.offer(kafkaPartitionOffset);
            }
        }
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
        running = false;
    }

    private void commitAll()
            throws Exception
    {
        Map<TopicAndPartition, Long> m = new HashMap<>();
        KafkaPartitionOffset osr = commitQueue.poll();
        while (null != osr) {
            TopicAndPartition tp = osr.getTopicPartition();
            Long x = m.get(tp);
            long offset = (null == x) ? osr.getOffset() : Math.max(x, osr.getOffset());
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
