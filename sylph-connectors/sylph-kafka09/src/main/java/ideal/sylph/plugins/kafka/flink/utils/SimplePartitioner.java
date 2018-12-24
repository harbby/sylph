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
package ideal.sylph.plugins.kafka.flink.utils;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class SimplePartitioner
        implements Partitioner
{
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
    {
        if (key != null) {
            String stringKey = key.toString();
            int offset = stringKey.hashCode();
            return Math.abs(offset % cluster.partitionCountForTopic(topic));
        }
        else {
            return 0;
        }
    }

    @Override
    public void close()
    {
    }

    @Override
    public void configure(Map<String, ?> configs)
    {
    }
}
