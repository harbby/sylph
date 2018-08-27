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
package ideal.common.memory;

import sun.misc.Unsafe;

public class RevisedObjectInHeap
{
    private long address = 0;

    private Unsafe unsafe = Platform.getUnsafe();

    // 让对象占用堆内存,触发[Full GC
    private byte[] bytes = null;

    public RevisedObjectInHeap()
    {
        address = unsafe.allocateMemory(2 * 1024 * 1024);  //2M堆外内存
        bytes = new byte[1024 * 1024];
    }

    @Override
    protected void finalize()
            throws Throwable
    {
        super.finalize();
        System.out.println("finalize. 释放堆外内存" + address);
        unsafe.freeMemory(address);
    }

    public static void main(String[] args)
    {
        while (true) {
            RevisedObjectInHeap heap = new RevisedObjectInHeap();
            //System.out.println("memory address=" + heap.address);
        }
    }
}
