package ideal.sylph.common.memory;

public class MemoryBlock
        extends MemoryLocation
{
    private final long length;
    public int pageNumber = -1;

    public MemoryBlock(Object obj, long offset, long length)
    {
        super(obj, offset);
        this.length = length;
    }

    /**
     * Returns the size of the memory block.
     */
    public long size()
    {
        return length;
    }

    /**
     * Fills the memory block with the specified byte value.
     */
    public void fill(byte value)
    {
        Platform.getUnsafe().setMemory(obj, offset, length, value);
    }
}
