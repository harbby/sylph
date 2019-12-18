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
package ideal.sylph.runner.flink;

import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.Binder;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;

public class FlinkBean
        implements Bean
{
    private final StreamTableEnvironment tableEnv;

    public FlinkBean(StreamTableEnvironment tableEnv)
    {
        this.tableEnv = tableEnv;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class, ((StreamTableEnvironmentImpl) tableEnv).execEnv());
        binder.bind(org.apache.flink.table.api.java.StreamTableEnvironment.class, tableEnv);
    }
}
