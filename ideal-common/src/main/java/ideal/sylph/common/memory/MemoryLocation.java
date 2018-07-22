package ideal.sylph.common.memory;

public class MemoryLocation
{
    Object obj;
    long offset;

    //< 适用于堆内内存
    public MemoryLocation(Object obj, long offset)
    {
        this.obj = obj;
        this.offset = offset;
    }

    //< 适用于堆外内存
    public MemoryLocation()
    {
        this(null, 0);
    }
}
