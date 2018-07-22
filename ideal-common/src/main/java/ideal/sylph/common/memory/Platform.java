package ideal.sylph.common.memory;

import sun.misc.Cleaner;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public final class Platform
{
    private Platform() {}

    private static final Unsafe _UNSAFE;

    public static final int BOOLEAN_ARRAY_OFFSET;

    public static final int BYTE_ARRAY_OFFSET;

    public static final int SHORT_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET;

    public static final int DOUBLE_ARRAY_OFFSET;

    public static Unsafe getUnsafe()
    {
        return _UNSAFE;
    }

    public static long reallocateMemory(long address, long oldSize, long newSize)
    {
        long newMemory = _UNSAFE.allocateMemory(newSize);
        copyMemory(null, address, null, newMemory, oldSize);
        _UNSAFE.freeMemory(address);
        return newMemory;
    }

    /**
     * Uses internal JDK APIs to allocate a DirectByteBuffer while ignoring the JVM's
     * MaxDirectMemorySize limit (the default limit is too low and we do not want to require users
     * to increase it).
     */
    @SuppressWarnings("unchecked")
    public static ByteBuffer allocateDirectBuffer(int size)
    {
        try {
            Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
            Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
            constructor.setAccessible(true);
            Field cleanerField = cls.getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            long memory = _UNSAFE.allocateMemory(size);
            ByteBuffer buffer = (ByteBuffer) constructor.newInstance(memory, size);
            Cleaner cleaner = Cleaner.create(buffer, () -> _UNSAFE.freeMemory(memory));
            cleanerField.set(buffer, cleaner);
            return buffer;
        }
        catch (Exception e) {
            throwException(e);
        }
        throw new IllegalStateException("unreachable");
    }

    public static void copyMemory(
            Object src, long srcOffset, Object dst, long dstOffset, long length)
    {
        // Check if dstOffset is before or after srcOffset to determine if we should copy
        // forward or backwards. This is necessary in case src and dst overlap.
        if (dstOffset < srcOffset) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        }
        else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
            }
        }
    }

    /**
     * Raises an exception bypassing compiler checks for checked exceptions.
     */
    public static void throwException(Throwable t)
    {
        _UNSAFE.throwException(t);
    }

    /**
     * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
     * allow safepoint polling during a large copy.
     */
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    static {
        sun.misc.Unsafe unsafe;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (sun.misc.Unsafe) unsafeField.get(null);
        }
        catch (Throwable cause) {
            unsafe = null;
        }
        _UNSAFE = unsafe;

        if (_UNSAFE != null) {
            BOOLEAN_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(boolean[].class);
            BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
            SHORT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(short[].class);
            INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
            LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
            FLOAT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(float[].class);
            DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
        }
        else {
            BOOLEAN_ARRAY_OFFSET = 0;
            BYTE_ARRAY_OFFSET = 0;
            SHORT_ARRAY_OFFSET = 0;
            INT_ARRAY_OFFSET = 0;
            LONG_ARRAY_OFFSET = 0;
            FLOAT_ARRAY_OFFSET = 0;
            DOUBLE_ARRAY_OFFSET = 0;
        }
    }
}
