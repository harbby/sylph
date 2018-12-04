package ideal.sylph.plugins.hbase.tuple;

import java.io.Serializable;

public abstract class Tuple implements Serializable {
    public abstract <A> A f0();
    public abstract <B> B f1();
}
