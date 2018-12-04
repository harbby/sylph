package ideal.sylph.plugins.hbase.tuple;

public class Tuple2<A, B> extends Tuple {
    private A a;
    private B b;

    public Tuple2(A a, B b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public A f0() {
        return a;
    }

    @Override
    public B f1() {
        return b;
    }

    @Override
    public String toString() {
        return "Tuple2{" +
                "a=" + a +
                ", b=" + b +
                '}';
    }
}
