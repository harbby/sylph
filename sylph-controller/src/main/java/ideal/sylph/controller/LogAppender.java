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
package ideal.sylph.controller;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/*
 * 基于logback的日志查询容器 use: readLines("uuid", nextIndex)
 */
public class LogAppender
        extends UnsynchronizedAppenderBase<ILoggingEvent>
{
    private final int maxSizeQueue;
    private final Layout<ILoggingEvent> layout;

    private final AtomicInteger pointer = new AtomicInteger(0);
    private final AtomicReferenceArray<String> datas;

    private final String consoleName = "webLogConsoleService";
    //groupId 如果请求的和服务器不一致 则返回全量的
    private final String uuid = String.valueOf(System.currentTimeMillis());

    public LogAppender(int maxSizeQueue)
    {
        this.maxSizeQueue = maxSizeQueue;
        this.datas = new AtomicReferenceArray<>(maxSizeQueue);

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        this.setName(consoleName);
        this.setContext(loggerContext);
        this.start();

        Logger logger = loggerContext.getLogger("ROOT");
        logger.addAppender(this);

        PatternLayout layout = new PatternLayout();
        layout.setContext(context);
        layout.setPattern("%d{yy-MM-dd HH:mm:ss} %p[%F:%L]-%m%n");
        layout.start();
        this.layout = layout;
    }

    public String getUuid()
    {
        return this.uuid;
    }

    @Override
    protected void append(ILoggingEvent event)
    {
        String msg = layout.doLayout(event);
        writeLine(msg);
    }

    private void writeLine(String line)
    {
        datas.set(pointer.getAndAdd(1), line);  //getAndAdd i++
        if (pointer.get() == maxSizeQueue) {  //循环置0
            pointer.set(0);
        }
    }

    /*
     * 阅读时需要传入 偏移量
     */
    public Map<String, Object> readLines(String uuid, int next)
    {
        if (next < 0 || next >= maxSizeQueue || !this.uuid.equals(uuid)) {
            return readAll();
        }
        List<String> builder = new ArrayList<>();
        if (next <= pointer.get()) {
            next = getByRange(next, pointer.get(), builder);
        }
        else {
            next = getByRange(0, pointer.get(), builder);
        }
        Map<String, Object> object = new HashMap<>();
        object.put("logs", builder);
        object.put("next", next);
        object.put("id", uuid);
        return object;
    }

    private Map<String, Object> readAll()
    {
        List<String> builder = new ArrayList<>();

        getByRange(pointer.get(), maxSizeQueue, builder);
        int next = getByRange(0, pointer.get(), builder);

        Map<String, Object> object = new HashMap<>();
        object.put("logs", builder);
        object.put("next", next);
        object.put("id", uuid);
        return object;
    }

    private int getByRange(int start, int stop, List<String> builder)
    {
        int next = start;
        for (; next < stop; next++) {
            String line = datas.get(next);
            if (line != null) {
                builder.add(line);
            }
        }
        return next;
    }
}
