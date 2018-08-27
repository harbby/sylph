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
package ideal.common.memory.offheap.collection;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class OffHeapMapTest
{
    private static final ExecutorService pool = Executors.newFixedThreadPool(5);
    private static final String msg = "this off head value dwah jdawhdaw dhawhdawhdawhjdawjd dhawdhaw djawdjaw";

    @Test
    public void testOffHeapMap()
    {
        final Map<String, String> offHeapMap = new OffHeapMap<>(
                (String str) -> str.getBytes(UTF_8),
                (byte[] bytes) -> new String(bytes, UTF_8)
        );
        offHeapMap.put("a1", msg);
        Assert.assertEquals(offHeapMap.get("a1"), msg);
        offHeapMap.clear();
    }
}