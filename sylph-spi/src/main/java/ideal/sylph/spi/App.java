package ideal.sylph.spi;

public interface App<T>
{
    T getContext();

    void build()
            throws Exception;
}
