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
package com.github.harbby.sylph.json;

import com.github.harbby.gadtry.compiler.JavaClassCompiler;
import com.github.harbby.gadtry.compiler.JavaSourceObject;
import com.github.harbby.sylph.api.Field;
import com.github.harbby.sylph.api.Schema;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class JsonReadCodeGenerator
{
    private static final List<String> KAFKA_COLUMNS = Arrays.asList("_topic", "_key", "_message", "_partition", "_offset");
    private String fullName;
    private byte[] byteCode;
    private final String className;
    private String code;

    public JsonReadCodeGenerator(String className)
    {
        this.className = className;
    }

    public void doCodeGen(Schema schema)
    {
        StringBuilder coder = new StringBuilder("package " + JsonPathReader.class.getPackage().getName() + ";\n" +
                "public class " + className + " extends " + JsonPathReader.class.getName() + " {\n" +
                "        @Override\n" +
                "        public void deserialize(" + KafkaRecord.class.getName() + "<byte[], byte[]> record, Object[] values) {\n" +
                "        int i =0;\n");
        for (Field field : schema.getFields()) {
            coder.append("values[i++] = ");
            if (KAFKA_COLUMNS.contains(field.getName())) {
                switch (field.getName()) {
                    case "_topic":
                        coder.append("record.topic();");
                        break;
                    case "_message":
                        coder.append("new String(record.value(), java.nio.charset.StandardCharsets.UTF_8);");
                        break;
                    case "_key":
                        coder.append("record.key() == null ? null : new String(record.key(), java.nio.charset.StandardCharsets.UTF_8);");
                        break;
                    case "_partition":
                        coder.append("record.partition();");
                        break;
                    case "_offset":
                        coder.append("record.offset();");
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            else {
                Optional<String> extend = Optional.ofNullable(field.getExtend());
                String jsonPath = extend.orElse("$." + field.getName());
                coder.append(String.format("this.read(\"%s\");", jsonPath));
            }
            coder.append("\n");
        }
        coder.append("}}");
        System.out.println(coder);
        ClassLoader classLoader = new URLClassLoader(new URL[0]);
        JavaClassCompiler compiler = new JavaClassCompiler(classLoader);
        JavaSourceObject target = compiler.doCompile(className, coder.toString(), Arrays.asList(
                "-source", "1.8",
                "-target", "1.8",
                "-cp", this.getClass().getProtectionDomain().getCodeSource().getLocation().toString()));

        this.fullName = JsonPathReader.class.getPackage().getName() + "." + className;
        this.byteCode = requireNonNull(target.getClassByteCodes().get(fullName), "byte code is null");
        this.code = coder.toString();
    }

    public byte[] getByteCode()
    {
        return byteCode;
    }

    public String getFullName()
    {
        return fullName;
    }

    public String getCode()
    {
        return code;
    }
}
