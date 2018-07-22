package ideal.sylph.common.memory;

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
