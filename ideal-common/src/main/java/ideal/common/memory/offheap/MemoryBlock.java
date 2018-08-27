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
package ideal.common.memory.offheap;

import sun.misc.Unsafe;

public final class MemoryBlock
        implements AutoCloseable
{
    private static final Unsafe unsafe = Platform.getUnsafe();
    private final long address;
    private final int maxOffset;
    private volatile boolean isFree = false;

    public MemoryBlock(byte[] value)
    {
        this.address = unsafe.allocateMemory(value.length);  //2 * 1024 * 1024=2M
        unsafe.setMemory(address, value.length, (byte) 0xa5);  //init
        this.maxOffset = value.length;

        for (int i = 0; i < value.length; i++) {
            unsafe.putByte(null, address + i, value[i]);
        }
    }

    public int getSize()
    {
        return maxOffset;
    }

    public byte[] getByteValue()
    {
        byte[] bytes = new byte[maxOffset];
        for (int i = 0; i < maxOffset; i++) {
            bytes[i] = unsafe.getByte(address + i);
        }

        return bytes;
    }

    private synchronized void free()
    {
        if (!isFree) {
            unsafe.setMemory(address, maxOffset, (byte) 0x5a);  //init
            unsafe.freeMemory(address);
            this.isFree = true;
        }
    }

    @Override
    protected void finalize()
            throws Throwable
    {
        this.free();
        super.finalize();
        //System.out.println(this.getClass() + "[time:" + System.currentTimeMillis() + "] finalize. gc 释放堆外内存" + address);
    }

    @Override
    public void close()
    {
        this.free();
    }
}
