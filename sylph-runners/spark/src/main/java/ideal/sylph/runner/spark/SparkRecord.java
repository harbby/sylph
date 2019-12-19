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
package ideal.sylph.runner.spark;

import ideal.sylph.etl.Record;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

public class SparkRecord
        implements Record
{
    private final Row row;

    public SparkRecord(Row row)
    {
        this.row = row;
    }

    public static SparkRecord make(Row row)
    {
        return new SparkRecord(row);
    }

    public static Row parserRow(Record record)
    {
        if (record instanceof SparkRecord) {
            return ((SparkRecord) record).get();
        }
        else if (record instanceof DefaultRecord) {
            //todo: schema field type
            return new GenericRow(((DefaultRecord) record).getValues());
        }
        else {
            throw new RuntimeException(" not souch row type: " + record.getClass());
        }
    }

    public Row get()
    {
        return row;
    }

    @Override
    public String mkString(String seq)
    {
        return row.mkString(seq);
    }

    @Override
    public <T> T getAs(String key)
    {
        return (T) row.getAs(key);
    }

    @Override
    public <T> T getAs(int i)
    {
        return (T) row.getAs(i);
    }

    @Override
    public int size()
    {
        return row.size();
    }

    @Override
    public String toString()
    {
        return row.toString();
    }
}
