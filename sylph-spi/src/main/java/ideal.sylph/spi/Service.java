package ideal.sylph.spi;

public interface Service
{
    void start()
            throws Exception;

    void close()
            throws Exception;
}
