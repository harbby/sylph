package ideal.sylph.common.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ObjectInputStreamProxy
        extends java.io.ObjectInputStream
{
    private ClassLoader classLoader;

    public ObjectInputStreamProxy(InputStream in)
            throws IOException
    {
        super(in);
    }

    /**
     * ObjectInputStreamProxy used by user classLoader
     * <p>
     *
     * @param classLoader used by loadObject
     */
    public ObjectInputStreamProxy(InputStream in, ClassLoader classLoader)
            throws IOException
    {
        super(in);
        this.classLoader = classLoader;
    }

    /**
     * get Method LatestUserDefinedLoader with java.io.ObjectInputStreamProxy
     * with jdk.internal.misc.VM.latestUserDefinedLoader()
     */
    public static ClassLoader getLatestUserDefinedLoader()
    {
        //super.latestUserDefinedLoader();
        Class<?> class1 = java.io.ObjectInputStream.class;
        try {
            Method method = class1.getDeclaredMethod("latestUserDefinedLoader");
            method.setAccessible(true);  //必须要加这个才能
            return (ClassLoader) method.invoke(null);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Not compatible with java version");
        }
    }

    /**
     * get field primClasses with java.io.ObjectInputStreamProxy
     */
    private static Map<String, Class<?>> getPrimClasses()
    {
        Class<?> class1 = java.io.ObjectInputStream.class;
        Map<String, Class<?>> primClasses = null;
        try {
            Field field = class1.getDeclaredField("primClasses");
            field.setAccessible(true);
            primClasses = (Map<String, Class<?>>) field.get(class1);
            return primClasses;
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Not compatible with java version");
        }
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException
    {
        if (classLoader == null) {
            return super.resolveClass(desc);
        }

        //return super.resolveClass(desc);
        String name = desc.getName();
        try {
            return Class.forName(name, false, classLoader);
        }
        catch (ClassNotFoundException ex) {
            Class<?> cl = getPrimClasses().get(name);
            if (cl != null) {
                return cl;
            }
            else {
                throw ex;
            }
        }
    }
}
