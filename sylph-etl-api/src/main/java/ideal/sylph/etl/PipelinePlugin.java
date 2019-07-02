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
package ideal.sylph.etl;

import java.io.Serializable;

public interface PipelinePlugin
        extends Serializable
{
    public static enum PipelineType
    {
        source(1),
        transform(2),
        sink(3),
        @Deprecated
        batch_join(4);

        private final int code;

        PipelineType(int i)
        {
            this.code = i;
        }
    }
}
