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
