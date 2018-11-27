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
package ideal.common.ioc;

import ideal.common.function.Creater;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IocFactoryTest
{
    @Test
    public void create()
    {
        IocFactory iocFactory = IocFactory.create(binder -> {
            binder.bind(Set.class).by(HashSet.class).withSingle();
            binder.bind(HashSet.class).withSingle();
            binder.bind(List.class).byCreater(ArrayList::new);  //Single object
            binder.bind(Object.class, new Object());
            binder.bind(Map.class).byCreater(HashMap::new).withSingle();  //Single object
            binder.bind(TestInject.class);
        });

        TestInject testInject = iocFactory.getInstance(TestInject.class);
        TestInject testInject2 = iocFactory.getInstance(TestInject.class);
        //Object a6546 = iocFactory.getAllBeans();

        Set a1 = iocFactory.getInstance(Set.class);
        Set a2 = iocFactory.getInstance(Set.class);
        Assert.assertEquals(true, a1 == a2); // Single object

        Map map1 = iocFactory.getInstance(Map.class);
        Map map2 = iocFactory.getInstance(Map.class);
        Assert.assertEquals(true, map1 == map2);  //Single object,单例对象
        Assert.assertEquals(false, iocFactory.getInstance(List.class) == iocFactory.getInstance(List.class));

        Assert.assertNotNull(iocFactory.getInstance(HashSet.class));

        Creater a5 = iocFactory.getCreater(HashSet.class);
        Creater a6 = iocFactory.getCreater(HashSet.class);
        Assert.assertEquals(false, a5 == a6);
        Assert.assertEquals(true, a5.get() == a6.get());
    }
}