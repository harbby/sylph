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
package ideal.sylph.runner.flink.sql;

import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.runner.flink.etl.FlinkRow;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class AsyncFunctionHelper
{
    private AsyncFunctionHelper() {}

    public static DataStream<Row> translate(
            DataStream<Row> inputStream,
            RealTimeTransForm transForm)
    {
        RowTypeInfo streamRowType = (RowTypeInfo) inputStream.getType();
        AsyncFunction<Row, Row> asyncFunction = new RichAsyncFunctionImpl(transForm, streamRowType);

        DataStream<Row> joinResultStream = AsyncDataStream.orderedWait(
                inputStream, asyncFunction,
                1000, TimeUnit.MILLISECONDS, // 超时时间
                100);  // 进行中的异步请求的最大数量

        return joinResultStream;
    }

    public static class RichAsyncFunctionImpl
            extends RichAsyncFunction<Row, Row>
            implements Serializable
    {
        private final RealTimeTransForm transForm;
        private final RowTypeInfo streamRowType;

        public RichAsyncFunctionImpl(RealTimeTransForm transForm, RowTypeInfo streamRowType)
        {
            this.transForm = transForm;
            this.streamRowType = streamRowType;
        }

        @Override
        public void open(Configuration parameters)
                throws Exception
        {
            super.open(parameters);
            RuntimeContext context = getRuntimeContext();

            // get parallelism id
            int partitionId = (context.getNumberOfParallelSubtasks() > 0) ?
                    (context.getIndexOfThisSubtask() + 1) : 0;

            transForm.open(partitionId, 0);
        }

        @Override
        public void asyncInvoke(Row input, ResultFuture<Row> asyncCollector)
                throws Exception
        {
            CompletableFuture<Collection<Row>> resultFuture = CompletableFuture.supplyAsync(() -> {
                List<Row> rows = new ArrayList<>();
                transForm.process(new FlinkRow(input, streamRowType), record -> rows.add(FlinkRow.parserRow(record)));
                return rows;
            });

            // 设置请求完成时的回调: 将结果传递给 collector
            resultFuture.whenComplete((result, error) -> {
                if (error != null) {
                    //todo: 这里可以加入开关 如果关联失败是否进行置空,默认情况 整个任务会直接结束
                    asyncCollector.completeExceptionally(error);
                }
                else {
                    //因为一条数据 可能join出来多条 所以结果是集合
                    Row row = Row.of("uid", "topic", "uid", 123L, "batch111", "batch222");
                    asyncCollector.complete(result);
                }
            });
        }

        @Override
        public void close()
                throws Exception
        {
            super.close();
            transForm.close(null);
        }
    }
}
