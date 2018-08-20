package ideal.sylph.common.jvm;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JVMLauncherTest
{
    @Test
    public void test1()
            throws IOException, ClassNotFoundException, JVMException
    {
        System.out.println("--- vm test ---");
        JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                .setCallable(() -> {
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println("************ job start ***************");
                    return 1;
                })
                .addUserjars(Collections.emptyList())
                .setConsole((msg) -> System.out.println(msg))
                .build();

        VmFuture<Integer> out = launcher.startAndGet();
        Assert.assertEquals(out.get().get().intValue(), 1);
    }

    @Test
    public void test2()
            throws IllegalAccessException
    {
        Class<?> class1 = java.io.ObjectInputStream.class;
        try {
            Field field = class1.getDeclaredField("primClasses"); //class1.getDeclaredField("primClasses");
            field.setAccessible(true);  //必须要加这个才能
            Map map = (Map) field.get(class1);
            System.out.println(field.get(map));

            System.out.println(field.getName());
            System.out.println(field.getType());
        }
        catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    private final static String hehe = "hehe";
    public final String xixi = "xixi";

    @Test
    public void test()
    {
        Field[] fields = JVMLauncherTest.class.getDeclaredFields();
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.getType().toString().endsWith("java.lang.String") && Modifier.isStatic(field.getModifiers())) {
                    System.out.println(field.getName() + " , " + field.get(JVMLauncherTest.class));
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void methodTest()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        Class<?> class1 = java.io.ObjectInputStream.class;
        Method method = class1.getDeclaredMethod("latestUserDefinedLoader", null);
        method.setAccessible(true);  //必须要加这个才能
        Object a1 = method.invoke(null);
        Assert.assertTrue(a1 instanceof ClassLoader);
    }
}