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
package ideal.sylph.plugins.kafka.spark.structured;

import com.github.harbby.gadtry.collection.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaSourceUtil
{
    private KafkaSourceUtil() {}

    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceUtil.class);

    /**
     * 下面这些参数 是结构化流官网 写明不支持的参数
     **/
    private static final List<String> filterKeys = ImmutableList.of(
            "kafka_group_id", "group.id",
            "key.deserializer",
            "value.deserializer",
            "key.serializer",
            "value.serializer",
            "enable.auto.commit",
            "interceptor.classes"
    );

    /**
     * 对配置进行解析变换
     **/
    private static Map<String, String> configParser(Map<String, Object> optionMap)
    {
        return optionMap.entrySet().stream().filter(x -> {
            if (filterKeys.contains(x.getKey())) {
                logger.warn("spark结构化流引擎 忽略参数:key[{}] value[{}]", x.getKey(), x.getValue());
                return false;
            }
            else if (x.getValue() == null) {
                logger.warn("spark结构化流引擎 忽略value null参数:key[{}] value[null]", x.getKey());
                return false;
            }
            else {
                return true;
            }
        }).collect(Collectors.toMap(
                k -> {
                    switch (k.getKey()) {
                        case "kafka_topic":
                            return "subscribe";
                        case "kafka_broker":
                            return "kafka.bootstrap.servers";
                        case "auto.offset.reset":
                            return "startingOffsets"; //注意结构化流上面这里有两个参数
                        default:
                            return k.getKey();
                    }
                },
                v -> v.getValue().toString()));
    }

    public static Dataset<Row> getSource(SparkSession spark, Map<String, Object> optionMap)
    {
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .options(configParser(optionMap))
                .load();
        return df;

        //    val columns = df.columns.map {
        //      case "key" => "CAST(key AS STRING) as key"
        //      case "value" => "CAST(value AS STRING) as value"
        //      case that => that
        //    }
        //    df.selectExpr(columns: _*) //对输入的数据进行 cast转换
    }
}
