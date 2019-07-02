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
package ideal.sylph.plugins.kafka.flink;

import com.github.harbby.gadtry.collection.mutable.MutableSet;
import ideal.sylph.etl.PipelinePlugin;

import java.util.Set;

public class Plugin
        implements ideal.sylph.etl.Plugin
{
    @Override
    public Set<Class<? extends PipelinePlugin>> getConnectors()
    {
        return MutableSet.<Class<? extends PipelinePlugin>>builder()
                .add(KafkaSource08.class)
                .build();
    }
}
