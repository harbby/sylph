package ideal.sylph.common.jvm;

import java.io.Serializable;
import java.util.concurrent.Callable;

public interface VmCallable<V extends Serializable>
        extends Callable<V>, Serializable
{
}
