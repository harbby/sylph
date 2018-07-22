package ideal.sylph.runner.flink.jvm;

import com.google.common.collect.ImmutableList;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.common.jvm.VmFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * vm test
 */
public class JVMLauncherTest
{

    @Before
    public void setUp()
            throws Exception
    {
    }

    @Test
    public void test1()
            throws IOException, ClassNotFoundException
    {
        JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                .setCallable(() -> {
                    System.out.println("vm start...");
                    System.out.println("vm stop...");
                    return 1;
                })
                .addUserjars(ImmutableList.of())
                .build();

        VmFuture<Integer> out = launcher.startAndGet();
        Assert.assertEquals(out.get().get().intValue(), 1);
    }
}