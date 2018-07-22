package ideal.sylph.common.memory;

public interface MemoryAllocator
{
    /**
     * Whether to fill newly allocated and deallocated memory with 0xa5 and 0x5a bytes respectively.
     * This helps catch misuse of uninitialized or freed memory, but imposes some overhead.
     */
    boolean MEMORY_DEBUG_FILL_ENABLED = Boolean.parseBoolean(
            System.getProperty("spark.memory.debugFill", "false"));

    // Same as jemalloc's debug fill values.
    byte MEMORY_DEBUG_FILL_CLEAN_VALUE = (byte) 0xa5;
    byte MEMORY_DEBUG_FILL_FREED_VALUE = (byte) 0x5a;

    /**
     * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
     * to be zeroed out (call `fill(0)` on the result if this is necessary).
     */
    MemoryBlock allocate(long size)
            throws OutOfMemoryError;

    void free(MemoryBlock memory);

    MemoryAllocator UNSAFE = new UnsafeMemoryAllocator();
}
