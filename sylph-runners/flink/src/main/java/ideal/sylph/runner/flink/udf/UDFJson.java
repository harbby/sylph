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
package ideal.sylph.runner.flink.udf;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import ideal.sylph.annotation.Name;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.IOException;

@Name("get_json_object")
public class UDFJson
        extends ScalarFunction
{
    private static final Configuration jsonConf = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);

    /**
     * @return json string or null
     */
    @SuppressWarnings("unchecked")
    public String eval(String jsonString, String pathString)
            throws IOException
    {
        if (jsonString == null) {
            return null;
        }
        if (!pathString.startsWith("$")) {
            pathString = "$." + pathString;
        }
        ReadContext context = JsonPath.using(jsonConf).parse(jsonString);
        Object value = context.read(pathString);

        if (value == null) {
            return null;
        }
        else if (value instanceof String) {
            return (String) value;
        }
        else {
            return value.toString();
        }
    }

    @Override
    public TypeInformation<String> getResultType(Class<?>[] signature)
    {
        return Types.STRING;
    }
}
