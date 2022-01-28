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

import com.github.harbby.sylph.api.Schema;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.api.TableContext;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.api.annotation.Version;
import com.github.harbby.sylph.json.ByteCodeClassLoader;
import com.github.harbby.sylph.json.JsonPathReader;
import com.github.harbby.sylph.json.JsonReadCodeGenerator;
import com.github.harbby.sylph.json.KafkaRecord;
import ideal.sylph.plugins.kafka.spark.structured.KafkaSourceUtil;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import static com.github.harbby.sylph.runner.spark.SQLHepler.schemaToSparkType;
import static com.google.common.base.Preconditions.checkState;

@Name("kafka")
@Version("1.0.0")
@Description("this spark kafka 0.10+ source inputStream")
public class StructuredKafkaSource
        implements Source<Dataset<Row>>
{
    private final transient SparkSession spark;
    private final transient KafkaSourceConfig config;
    private final transient TableContext context;

    public StructuredKafkaSource(SparkSession spark, KafkaSourceConfig config, TableContext context)
    {
        this.spark = spark;
        this.config = config;
        this.context = context;
    }

    @Override
    public Dataset<Row> createSource()
    {
        String topics = config.getTopics();
        String brokers = config.getBrokers();
        String groupId = config.getGroupid();
        String offsetMode = config.getOffsetMode();

        checkState(!"largest".equals(offsetMode), "kafka 0.10+, use latest");
        checkState(!"smallest".equals(offsetMode), "kafka 0.10+, use earliest");

        Map<String, Object> kafkaParams = new HashMap<>(config.getOtherConfig());
        kafkaParams.put("subscribe", topics);
        kafkaParams.put("kafka.bootstrap.servers", brokers);
        kafkaParams.put("startingOffsets", offsetMode); //latest   earliest

        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class.getName()); //StringDeserializer
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class.getName()); //StringDeserializer

        Dataset<Row> inputStream = KafkaSourceUtil.getSource(spark, kafkaParams);
        checkState("json".equalsIgnoreCase(config.getValueType()), "only support json message");

        Schema schema = context.getSchema();
        JsonReadCodeGenerator codeGenerator = new JsonReadCodeGenerator("SparkKafkaJsonCodeGenReader");
        codeGenerator.doCodeGen(schema);
        StructType rowTypeInfo = schemaToSparkType(schema);
        JsonReaderMap jsonReaderMap = new JsonReaderMap(codeGenerator.getFullName(), codeGenerator.getByteCode(), schema.size());
        return inputStream.map(jsonReaderMap, RowEncoder.apply(rowTypeInfo)).filter(new FilterFunction<Row>()
        {
            private static final long serialVersionUID = -8446920382161348486L;

            @Override
            public boolean call(Row row)
                    throws Exception
            {
                return row != null;
            }
        });
    }

    private static class KafkaRecordWrapper
            extends KafkaRecord<byte[], byte[]>
    {
        private Row record;

        @Override
        public String topic()
        {
            return record.<String>getAs("topic");
        }

        @Override
        public Integer partition()
        {
            return record.<Integer>getAs("partition");
        }

        @Override
        public byte[] key()
        {
            return record.getAs("key");
        }

        @Override
        public byte[] value()
        {
            return record.getAs("value");
        }

        @Override
        public Long offset()
        {
            return record.<Long>getAs("offset");
        }
    }

    private static class JsonReaderMap
            implements MapFunction<Row, Row>, Serializable
    {
        private static final long serialVersionUID = -6661888963466437001L;
        private final String classFullName;
        private final byte[] byteCode;
        private transient JsonPathReader jsonPathReader;
        private transient KafkaRecordWrapper kafkaRecordWrapper;
        private final int columnSize;

        private JsonReaderMap(String classFullName, byte[] byteCode, int columnSize)
        {
            this.classFullName = classFullName;
            this.byteCode = byteCode;
            this.columnSize = columnSize;
        }

        @Override
        public Row call(Row record)
                throws Exception
        {
            byte[] message = record.getAs("value");
            if (message == null) {
                return null;
            }
            if (jsonPathReader == null) {
                this.jsonPathReader = createJsonDeserializer();
                this.kafkaRecordWrapper = new KafkaRecordWrapper();
            }
            jsonPathReader.initNewMessage(message);
            Object[] values = new Object[columnSize];
            kafkaRecordWrapper.record = record;
            jsonPathReader.deserialize(kafkaRecordWrapper, values);
            return new GenericRow(values);
        }

        private JsonPathReader createJsonDeserializer()
        {
            ByteCodeClassLoader loader = new ByteCodeClassLoader(Thread.currentThread().getContextClassLoader());
            Class<? extends JsonPathReader> codeGenClass = loader.defineClass(classFullName, byteCode).asSubclass(JsonPathReader.class);
            try {
                return codeGenClass.getConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new IllegalStateException("kafka code gen error");
            }
        }
    }
}
