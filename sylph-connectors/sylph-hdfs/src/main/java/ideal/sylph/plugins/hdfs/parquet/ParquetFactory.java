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
package ideal.sylph.plugins.hdfs.parquet;

import com.google.common.collect.ImmutableList;
import ideal.sylph.etl.Record;
import ideal.sylph.plugins.hdfs.factory.HDFSFactorys;
import ideal.sylph.plugins.hdfs.factory.TimeParser;
import ideal.sylph.plugins.hdfs.utils.CommonUtil;
import ideal.sylph.plugins.hdfs.utils.MemoryUtil;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * 这个是parquet的工厂
 */
public class ParquetFactory
        implements HDFSFactory
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetFactory.class);
    private static final short TIME_Granularity = 5;

    private final BlockingQueue<Runnable> streamData = new LinkedBlockingQueue<>(1000);
    private final BlockingQueue<Runnable> monitorEvent = new ArrayBlockingQueue<>(1000); //警报事件队列
    private final ExecutorService executorPool = Executors.newFixedThreadPool(300); //最多300个线程
    //---parquet流 工厂--结构:Map[key=table+day+0900,parquetWtiter]-
    private final Map<String, ApacheParquet> parquetManager = new HashMap<>();
    private final String hostName = CommonUtil.getHostNameOrPid();
    private final String writeTableDir;
    private final String table;
    private final MessageType schema;
    private final ParquetProperties.WriterVersion parquetVersion;

    private volatile boolean closed = false;

    /**
     * 默认的规则
     **/
    private static final List<CheckHandler> filterDefaultFuncs =
            ImmutableList.<CheckHandler>builder()
                    .add((key, theLastKey, writer) ->
                            key.equals(theLastKey) && new TimeParser(writer.getCreatedTime()).getWriterKey().equals(theLastKey))
                    .add((key, theLastKey, writer) ->
                            key.compareTo(theLastKey) <= 0 && writer.getCooldownTime() / 1000 / 60 >= 2) //晚到流 超过2分钟没有写入数据 就关闭
                    .add((key, theLastKey, writer) ->
                            (System.currentTimeMillis() - writer.getCreatedTime()) / 1000 / 60 >= 6) //超过6分钟的流 就关闭
                    .build();

    public interface CheckHandler
    {
        boolean apply(String key, String theLastKey, FileWriter writer);
    }

    public ParquetFactory(
            final String writeTableDir,
            final String table,
            ParquetProperties.WriterVersion parquetVersion,
            MessageType schema)
    {
        requireNonNull(writeTableDir, "writeTableDir is null");
        this.writeTableDir = writeTableDir.endsWith("/") ? writeTableDir : writeTableDir + "/";

        this.table = requireNonNull(table, "table is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.parquetVersion = requireNonNull(parquetVersion, "parquetVersion is null");

        /**
         * 消费者
         * */
        final Callable<Void> consumer = () -> {
            Thread.currentThread().setName("Parquet_Factory_Consumer");
            try {
                while (!closed) {
                    Runnable value = streamData.poll();
                    //事件1
                    if (value != null) {
                        value.run(); //put data line
                    }
                    //事件2 读取指示序列
                    Runnable event = monitorEvent.poll();
                    if (event != null) {
                        event.run();
                    }
                    //事件3
                    if (value == null && event == null) {
                        TimeUnit.MILLISECONDS.sleep(1);
                    }
                }
            }
            catch (Exception e) {
                logger.error("Parquet_Factory_Consumer error", e);
                System.exit(-1);
            }
            return null;
        };

        //register consumer
        executorPool.submit(consumer);
        //register monitor
        executorPool.submit(monitor);

        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook));
    }

    private final Runnable shutdownHook = () -> {
        closed = true;
        synchronized (parquetManager) {
            parquetManager.entrySet().stream().parallel().forEach(x -> {
                String rowKey = x.getKey();
                try {
                    x.getValue().close();
                }
                catch (IOException e) {
                    logger.error("addShutdownHook close textFile Writer failed {}", rowKey, e);
                }
            });
        }
    };

    /**
     * 新的 处理内存超载事件
     * 按流的大小排序关一半 优先关掉数据量大的流
     * 会阻塞消费者 形成反压
     **/
    public Runnable closeHalf = () ->
    {
        int cnt = parquetManager.size() / 3; //按创建时间排序关一半  //getCreatedTime
        AtomicInteger i = new AtomicInteger(0);
        parquetManager.entrySet().stream()
                .sorted((x, y) -> (int) (x.getValue().getDataSize() - y.getValue().getDataSize()))
                .parallel()
                .forEach(it -> {
                    String rowKey = it.getKey();
                    if (i.getAndIncrement() < cnt) {
                        parquetManager.remove(rowKey);
                        try {
                            it.getValue().close();
                        }
                        catch (IOException e) {
                            logger.info("parquet关闭失败 path:{}", it.getValue().getWritePath(), e);
                        }
                    }
                });
    };

    private final Callable<Void> monitor = () -> {
        Thread.currentThread().setName("Parquet_Factory_Monitor");
        while (!closed) {
            try {
                TimeUnit.SECONDS.sleep(5);
                checkflushRule(); //按照规则进行check出过期的parquet流
                if (MemoryUtil.checkMemory()) {
                    monitorEvent.put(closeHalf); //触发了 oom检测警告 ,将采用closeHalf处理
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    };

    /**
     * 把冷却时间大于5分钟的 都干掉
     * 关闭 指定toptic的指定 时间粒度
     **/
    private void checkflushRule()
    {
        // 允许额外延迟1分钟
        String theLastKey = table + HDFSFactorys.getRowKey(table, new TimeParser(new DateTime().minusMinutes(TIME_Granularity + 1)));

        List<Map.Entry<String, ApacheParquet>> closeWriters = parquetManager.entrySet().stream().filter(it -> {
            String key = it.getKey();
            ApacheParquet writer = it.getValue();
            return filterDefaultFuncs.stream().map(x -> x.apply(key, theLastKey, writer))
                    .reduce((x, y) -> x || y)
                    .orElse(false);
        }).collect(Collectors.toList());

        if (!closeWriters.isEmpty()) {
            monitorEvent.offer(() ->
                    closeWriters.forEach(x -> {
                        parquetManager.remove(x.getKey());
                        ApacheParquet writer = x.getValue();
                        executorPool.submit(() -> {
                            int count = ((ThreadPoolExecutor) executorPool).getActiveCount();
                            logger.info("正在关闭流个数:" + count + " 添加关闭流:" + writer.getWritePath());
                            try {
                                writer.close();
                            }
                            catch (IOException e) {
                                throw new RuntimeException("流关闭出错:", e);
                            }
                        });
                    })
            );
            //打印内存情况
            logger.info(MemoryUtil.getMemoryInfo(hostName));
        }
    }

    @Override
    public void writeLine(long eventTime, Map<String, Object> evalRow)
    {
        try {
            streamData.put(() -> {
                ApacheParquet parquet = getParquetWriter(eventTime);
                parquet.writeLine(evalRow);
            });
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void writeLine(long eventTime, Collection<Object> evalRow)
    {
        try {
            streamData.put(() -> {
                ApacheParquet parquet = getParquetWriter(eventTime);
                parquet.writeLine(evalRow);
            });
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void writeLine(long eventTime, Record evalRecord)
    {
        try {
            streamData.put(() -> {
                ApacheParquet parquet = getParquetWriter(eventTime);
                parquet.writeLine(evalRecord);
            });
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close()
    {
        closed = true;
        //------关闭所有的流-----
        // 此处存在线程安全问题,可能导致程序关闭时 丢失数据
        shutdownHook.run();
    }

    @Override
    public String getWriteDir()
    {
        return writeTableDir;
    }

    /**
     * rowKey = table + 5minute
     */
    private ApacheParquet getParquetWriter(String rowKey, Supplier<ApacheParquet> builder)
    {
        //2,检查流是否存在 不存在就新建立一个
        ApacheParquet writer = parquetManager.get(rowKey);
        if (writer != null) {
            return writer;
        }
        else {
            synchronized (parquetManager) {
                return parquetManager.computeIfAbsent(rowKey, (key) -> builder.get());
            }
        }
    }

    private ApacheParquet getParquetWriter(long eventTime)
    {
        TimeParser timeParser = new TimeParser(eventTime);
        String parquetPath = writeTableDir + timeParser.getPartitionPath();

        String rowKey = HDFSFactorys.getRowKey(table, timeParser);
        return getParquetWriter(rowKey, () -> {
            try {
                return ApacheParquet.create()
                        .parquetVersion(parquetVersion)
                        .schema(schema)
                        .writePath(parquetPath)
                        .get();
            }
            catch (IOException e) {
                throw new RuntimeException("parquet writer create failed", e);
            }
        });
    }
}
