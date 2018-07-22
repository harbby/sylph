package ideal.sylph.common.memory;

import org.junit.Test;

import java.io.BufferedOutputStream;
import java.nio.ByteBuffer;

public class MemoryAllocatorTest
{
    @Test
    public void allocate()
    {
        MemoryBlock block = MemoryAllocator.UNSAFE.allocate(12);
        block.fill((byte) 1);
        ByteBuffer byteBuffer = ByteBuffer.allocate(12);
        byteBuffer.put(new byte[] {});
    }

    @Override
    protected void finalize()
            throws Throwable
    {
        // 释放堆外内存
        super.finalize();
    }

    @Test
    public void free()
    {
    }
}