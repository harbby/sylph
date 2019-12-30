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
package ideal.sylph.plugins.hdfs.utils;

import ideal.sylph.etl.Field;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ParquetUtilTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void testBuildSchema()
    {
        List<Field> arrayList = Arrays.asList(
                new Field("String", String.class),
                new Field("Byte", Byte.class), new Field("Short", Short.class),
                new Field("Integer", Integer.class),
                new Field("Long", Long.class),
                new Field("Date", Date.class),
                new Field("Float", Float.class),
                new Field("Double", Double.class),
                new Field("BigDecimal", BigDecimal.class),
                new Field("Boolean", Boolean.class),
                new Field("byte[]", byte[].class));

        String actual = "message row { optional binary String (UTF8); " +
                "optional INT32 Byte; optional INT32 Short; " +
                "optional INT32 Integer; optional INT64 Long; " +
                "optional INT64 Date; optional FLOAT Float; " +
                "optional DOUBLE Double; optional DOUBLE BigDecimal; " +
                "optional BOOLEAN Boolean; optional binary byte[]; } ";

        Assert.assertEquals(actual, ParquetUtil.buildSchema(arrayList));
    }

    @Test
    public void testBuildSchemaThrowsExceptionGiveMapType()
    {
        thrown.expect(UnsupportedOperationException.class);
        ParquetUtil.buildSchema(Arrays.asList(new Field("Map", Map.class)));
    }

    @Test
    public void testBuildSchemaThrowsException()
    {
        thrown.expect(UnsupportedOperationException.class);
        ParquetUtil.buildSchema(Arrays.asList(new Field("Object", Object.class)));
    }
}
