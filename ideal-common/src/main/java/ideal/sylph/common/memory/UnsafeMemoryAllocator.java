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
package ideal.sylph.common.memory;

import static java.util.Objects.requireNonNull;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator
        implements MemoryAllocator
{
    @Override
    public MemoryBlock allocate(long size)
            throws OutOfMemoryError
    {
        long address = Platform.getUnsafe().allocateMemory(size);
        MemoryBlock memory = new MemoryBlock(null, address, size);
        if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
        }
        return memory;
    }

    @Override
    public void free(MemoryBlock memory)
    {
        requireNonNull(memory.obj, "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?");
        if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
        }
        Platform.getUnsafe().freeMemory(memory.offset);
    }
}
