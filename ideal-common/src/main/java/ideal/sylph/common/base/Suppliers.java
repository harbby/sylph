package ideal.sylph.common.base;

import java.io.Serializable;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class Suppliers
{
    private Suppliers() {}

    public static <T> Supplier<T> memoize(Supplier<T> delegate)
    {
        return delegate instanceof Suppliers.MemoizingSupplier ?
                delegate :
                new Suppliers.MemoizingSupplier<>(requireNonNull(delegate));
    }

    public static <T> Supplier<T> goLazy(Supplier<T> delegate)
    {
        return delegate instanceof Suppliers.MemoizingSupplier ?
                delegate :
                new Suppliers.MemoizingSupplier<>(requireNonNull(delegate));
    }

    static class MemoizingSupplier<T>
            implements Supplier<T>, Serializable
    {
        private final Supplier<T> delegate;
        private transient volatile boolean initialized = false;
        private transient T value;
        private static final long serialVersionUID = 0L;

        MemoizingSupplier(Supplier<T> delegate)
        {
            this.delegate = delegate;
        }

        public T get()
        {
            if (!this.initialized) {
                synchronized (this) {
                    if (!this.initialized) {
                        T t = this.delegate.get();
                        this.value = t;
                        this.initialized = true;
                        return t;
                    }
                }
            }

            return this.value;
        }

        public String toString()
        {
            return "Suppliers.memoize(" + this.delegate + ")";
        }
    }
}
