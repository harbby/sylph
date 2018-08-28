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
package ideal.sylph.runner.flink.actuator;

import ideal.sylph.parser.tree.WaterMark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class StreamSqlUtil
{
    private StreamSqlUtil() {}

    static DataStream<Row> checkStream(DataStream<Row> inputStream, RowTypeInfo tableTypeInfo)
    {
        TypeInformation<Row> sourceType = inputStream.getType();
        if (sourceType instanceof RowTypeInfo) {
            List<Integer> indexs = Arrays.stream(tableTypeInfo.getFieldNames())
                    .map(fieldName -> {
                        int fieldIndex = ((RowTypeInfo) sourceType).getFieldIndex(fieldName);
                        checkState(fieldIndex != -1, sourceType + " not with " + fieldName);
                        return fieldIndex;
                    })
                    .collect(Collectors.toList());
            return inputStream.map(inRow -> Row.of(indexs.stream().map(inRow::getField).toArray()))
                    .returns(tableTypeInfo);
        }
        else {
            throw new RuntimeException("sourceType not is RowTypeInfo");
        }
    }

    static DataStream<Row> buildWaterMark(
            WaterMark waterMark,
            RowTypeInfo tableTypeInfo,
            DataStream<Row> dataStream)
    {
        String fieldName = waterMark.getFieldName();
        int fieldIndex = tableTypeInfo.getFieldIndex(fieldName);
        checkState(fieldIndex != -1, tableTypeInfo + " not with " + fieldName);
        if (waterMark.getOffset() instanceof WaterMark.RowMaxOffset) {
            long offset = ((WaterMark.RowMaxOffset) waterMark.getOffset()).getOffset();
            return dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>()
            {
                private final long maxOutOfOrderness = offset;  // 5_000L;//最大允许的乱序时间是5s
                private long currentMaxTimestamp = Long.MIN_VALUE;

                @Override
                public long extractTimestamp(Row element, long previousElementTimestamp)
                {
                    Long time = (Long) requireNonNull(element.getField(fieldIndex),
                            String.format("row[%s] field %s[index: %s] is null", element, fieldName, fieldIndex));
                    this.currentMaxTimestamp = Math.max(currentMaxTimestamp, time);
                    return time;
                }

                @Nullable
                @Override
                public Watermark getCurrentWatermark()
                {
                    // return the watermark as current highest timestamp minus the out-of-orderness bound
                    return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                }
            }).returns(tableTypeInfo);
        }
        else if (waterMark.getOffset() instanceof WaterMark.SystemOffset) {
            long offset = ((WaterMark.SystemOffset) waterMark.getOffset()).getOffset();
            return dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>()
            {
                private final long maxOutOfOrderness = offset;  // 5_000L;//最大允许的乱序时间是5s

                @Override
                public long extractTimestamp(Row element, long previousElementTimestamp)
                {
                    Long time = (Long) requireNonNull(element.getField(fieldIndex),
                            String.format("row[%s] field %s[index: %s] is null", element, fieldName, fieldIndex));
                    return time;
                }

                @Nullable
                @Override
                public Watermark getCurrentWatermark()
                {
                    return new Watermark(System.currentTimeMillis() - maxOutOfOrderness);
                }
            }).returns(tableTypeInfo);
        }
        else {
            throw new UnsupportedOperationException("this " + waterMark + " have't support!");
        }
    }
}
