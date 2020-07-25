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

import ideal.sylph.parser.antlr.tree.AllowedLateness;
import ideal.sylph.parser.antlr.tree.Identifier;
import ideal.sylph.parser.antlr.tree.SelectQuery;
import ideal.sylph.parser.antlr.tree.WindowTrigger;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class TriggerWindowHelper
{
    private final Set<Integer> compiledNodes = new HashSet<>();

    public void settingTrigger(StreamGraph streamGraph, Optional<WindowTrigger> windowTrigger, Optional<AllowedLateness> allowedLateness)
            throws NoSuchFieldException, IllegalAccessException
    {
        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            StreamOperator<?> streamOperator = streamNode.getOperator();
            if (streamOperator instanceof WindowOperator) {
                if (!compiledNodes.contains(streamNode.getId())) {
                    setting((WindowOperator) streamOperator, windowTrigger, allowedLateness);
                    compiledNodes.add(streamNode.getId());
                }
            }
        }
    }

    public void settingTrigger(StreamGraph streamGraph, SelectQuery selectQuery)
    {
        Optional<WindowTrigger> windowTrigger = Optional.empty();
        Optional<AllowedLateness> allowedLateness = Optional.empty();

        List<SelectQuery> selectQueries = new ArrayList<>();
        findAllSelectQuery(selectQuery, selectQueries);
        for (SelectQuery query : selectQueries) {
            if (query.getWindowTrigger().isPresent() || query.getAllowedLateness().isPresent()) {
                checkState(!windowTrigger.isPresent() && !allowedLateness.isPresent(), "only");
                windowTrigger = query.getWindowTrigger();
                allowedLateness = query.getAllowedLateness();
            }
        }

        try {
            this.settingTrigger(streamGraph, windowTrigger, allowedLateness);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void findAllSelectQuery(SelectQuery selectQuery, List<SelectQuery> result)
    {
        result.add(selectQuery);
        for (Map.Entry<Identifier, SelectQuery> entry : selectQuery.getWithTableQuery().entrySet()) {
            findAllSelectQuery(entry.getValue(), result);
        }
    }

    private void setting(WindowOperator windowOperator, Optional<WindowTrigger> windowTrigger, Optional<AllowedLateness> allowedLatenessOptional)
            throws NoSuchFieldException, IllegalAccessException
    {
        if (allowedLatenessOptional.isPresent()) {
            long allowedLateness = Time.seconds(allowedLatenessOptional.get().getAllowedLateness()).toMilliseconds();

            Field field = WindowOperator.class.getDeclaredField("allowedLateness");
            field.setAccessible(true);
            field.set(windowOperator, allowedLateness);
        }
        if (windowTrigger.isPresent()) {
            long cycleTime = windowTrigger.get().getCycleTime();
            Trigger<?, ? extends Window> trigger = ContinuousProcessingTimeTrigger.of(Time.seconds(cycleTime));

            Field field = WindowOperator.class.getDeclaredField("trigger");
            field.setAccessible(true);
            field.set(windowOperator, trigger);
        }
    }
}
