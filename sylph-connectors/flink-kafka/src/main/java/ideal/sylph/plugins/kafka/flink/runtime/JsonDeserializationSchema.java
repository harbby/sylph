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
package ideal.sylph.plugins.kafka.flink.runtime;

import com.github.harbby.sylph.api.Schema;
import com.github.harbby.sylph.json.ByteCodeClassLoader;
import com.github.harbby.sylph.json.JsonPathReader;
import com.github.harbby.sylph.json.KafkaRecord;
import com.github.harbby.sylph.runner.flink.engines.StreamSqlUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class JsonDeserializationSchema
        implements KafkaDeserializationSchema<Row>, Serializable
{
    private static final long serialVersionUID = 5510055442667232054L;

    private final RowTypeInfo rowTypeInfo;
    private transient JsonPathReader jsonPathReader;
    private transient KafkaRecordWrapper kafkaRecordWrapper;
    private final Schema schema;
    private final byte[] byteCode;
    private final String code;
    private final String className;

    public JsonDeserializationSchema(Schema schema, String className, byte[] byteCode, String code)
    {
        this.schema = schema;
        this.rowTypeInfo = StreamSqlUtil.schemaToRowTypeInfo(schema);
        this.byteCode = byteCode;
        this.code = code;
        this.className = className;
    }

    @Override
    public boolean isEndOfStream(Row nextElement)
    {
        return false;
    }

    @Override
    public Row deserialize(ConsumerRecord<byte[], byte[]> record)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out)
            throws Exception
    {
        byte[] message = record.value();
        if (message == null) {
            return;
        }
        if (jsonPathReader == null) {
            this.jsonPathReader = createJsonDeserializer();
            this.kafkaRecordWrapper = new KafkaRecordWrapper();
        }
        jsonPathReader.initNewMessage(message);
        Object[] values = new Object[schema.size()];

        kafkaRecordWrapper.record = record;
        jsonPathReader.deserialize(kafkaRecordWrapper, values);
        out.collect(Row.of(values));
    }

    private JsonPathReader createJsonDeserializer()
    {
        ByteCodeClassLoader loader = new ByteCodeClassLoader(Thread.currentThread().getContextClassLoader());
        Class<? extends JsonPathReader> codeGenClass = loader.defineClass(className, byteCode).asSubclass(JsonPathReader.class);
        try {
            return codeGenClass.getConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("kafka code gen error");
        }
    }

    @Override
    public TypeInformation<Row> getProducedType()
    {
        return rowTypeInfo;
    }

    private static class KafkaRecordWrapper
            extends KafkaRecord<byte[], byte[]>
    {
        private ConsumerRecord<byte[], byte[]> record;

        @Override
        public String topic()
        {
            return record.topic();
        }

        @Override
        public Integer partition()
        {
            return record.partition();
        }

        @Override
        public byte[] key()
        {
            return record.key();
        }

        @Override
        public byte[] value()
        {
            return record.value();
        }

        @Override
        public Long offset()
        {
            return record.offset();
        }
    }
}
