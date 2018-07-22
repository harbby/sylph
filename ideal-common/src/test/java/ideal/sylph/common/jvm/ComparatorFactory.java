package ideal.sylph.common.jvm;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.function.UnaryOperator;

public class ComparatorFactory
{
    public static UnaryOperator<Integer> makeComparator()
    {
        UnaryOperator<Integer> func = (UnaryOperator<Integer> & Serializable) (a) -> a + 1;

        return func;
    }

    @Test
    public void Java8TypeIntersectionTest()
    {
        UnaryOperator<Integer> func = makeComparator();
        Assert.assertEquals(func.apply(1).intValue(), 2);
        Assert.assertTrue(func instanceof Serializable);
    }
}