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
package ideal.common.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * yarn job 容器 需要进行动态代理
 */
public class DynamicProxy
        implements InvocationHandler
{
    //　这个就是我们要代理的真实对象
    private Object subject;

    //    构造方法，给我们要代理的真实对象赋初值
    public DynamicProxy(Object subject)
    {
        this.subject = subject;
    }

    public Object getProxy()
    {
        return getProxy(subject.getClass().getInterfaces());
    }

    public Object getProxy(Class<?>... interfaces)
    {
        /*
         * 通过Proxy的newProxyInstance方法来创建我们的代理对象，我们来看看其三个参数
         * 第一个参数 handler.getClass().getClassLoader() ，我们这里使用handler这个类的ClassLoader对象来加载我们的代理对象
         * 第二个参数realSubject.getClass().getInterfaces()，我们这里为代理对象提供的接口是真实对象所实行的接口，表示我要代理的是该真实对象，这样我就能调用这组接口中的方法了
         * 第三个参数handler， 我们这里将这个代理对象关联到了上方的 InvocationHandler 这个对象上
         */
        return Proxy.newProxyInstance(this.getClass().getClassLoader(), interfaces, this);
    }

    // 该方法负责集中处理动态代理类上的所有方法调用。第一个参数既是代理类实例，第二个参数是被调用的方法对象
    // 第三个方法是调用参数。调用处理器根据这三个参数进行预处理或分派到委托类实例上反射执行
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        //　　在代理真实对象前我们可以添加一些自己的操作
        //System.out.println("before rent house");
        //System.out.println("Method:" + method);

        //    当代理对象调用真实对象的方法时，其会自动的跳转到代理对象关联的handler对象的invoke方法来进行调用
        Object obj = method.invoke(subject, args);
        //System.out.println("我是方法:"+method.getName()); //可以用来判断 是否是某个方法

        //　　在代理真实对象后我们也可以添加一些自己的操作
        //System.out.println("after rent house");
        return obj;
    }
}
