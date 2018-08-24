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
package ideal.sylph.spi;

import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;

import java.util.List;

/**
 * 用来描述一个job的详细结构
 * 可以转换为多种结构(json 和source-trans-sink 和graph)
 */
public abstract class EtlFlow
        extends Flow
{
    public abstract List<EdgeInfo> getEdges();

    public abstract List<NodeInfo> getNodes();
}
