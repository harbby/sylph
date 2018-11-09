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
package ideal.sylph.plugins.mysql;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.api.RealTimeSink;

@Name("console")
@Description("print data line console")
public class PrintSink
        implements RealTimeSink
{
    @Override
    public boolean open(long partitionId, long version)
    {
        return true;
    }

    @Override
    public void close(Throwable errorOrNull)
    {
    }

    @Override
    public void process(Row value)
    {
        System.out.println(value.mkString());
    }
}
