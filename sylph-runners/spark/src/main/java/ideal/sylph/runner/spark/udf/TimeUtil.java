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
package ideal.sylph.runner.spark.udf;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.sql.Timestamp;
import java.util.Arrays;

public class TimeUtil
{
    @Name("date_format")
    @Description("date_format('2018-01-01 12:00:00', 'yyyy-MM-dd HH:mm:ss')-> TIMESTAMP")
    public static class DateFormatUDF
    {
        public DataType getResultType(Class<?>[] signature)
        {
            return Arrays.equals(signature, new Class[] {String.class, String.class}) ? DataTypes.TimestampType : DataTypes.StringType;
        }

        public String eval(Timestamp time, String toFormat)
        {
            return (new DateTime(time)).toString(toFormat);
        }

        public String eval(String time, String fromFormat, String toFormat)
        {
            return DateTimeFormat.forPattern(fromFormat).parseDateTime(time).toString(toFormat);
        }

        public Timestamp eval(String time, String fromFormat)
        {
            return new Timestamp(DateTimeFormat.forPattern(fromFormat).parseDateTime(time).getMillis());
        }
    }

    @Name("from_unixtime")
    @Description("from_unixtime(long)-> TIMESTAMP or from_unixtime(long 13time,varchar to_format)-> varchar")
    public static class FromUnixTime
            implements UDF2<Long, String, String>
    {
        public DataType getResultType(Class<?>[] signature)
        {
            return signature.length == 2 ? DataTypes.StringType : DataTypes.TimestampType;
        }

        public Timestamp eval(long time)
        {
            return new Timestamp(time);
        }

        public String eval(long time, String format)
        {
            return (new DateTime(time)).toString(format);
        }

        @Override
        public String call(Long time, String format)
                throws Exception
        {
            return eval(time, format);
        }
    }
}
