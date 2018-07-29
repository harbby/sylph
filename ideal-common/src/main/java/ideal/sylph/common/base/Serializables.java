package ideal.sylph.common.base;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Serializables
{
    private Serializables() {}

    public static byte[] serialize(Serializable serializable)
            throws IOException
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream os = new ObjectOutputStream(bos)
        ) {
            os.writeObject(serializable);
            return bos.toByteArray();
        }
    }

    public static Object byteToObject(byte[] bytes)
            throws IOException, ClassNotFoundException
    {
        return byteToObject(bytes, null);
    }

    public static Object byteToObject(byte[] bytes, ClassLoader classLoader)
            throws IOException, ClassNotFoundException
    {
        try (ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
                ObjectInputStreamProxy oi = new ObjectInputStreamProxy(bi, classLoader)
        ) {
            return oi.readObject();
        }
    }
}
