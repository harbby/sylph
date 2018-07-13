package ideal.sylph.common.jvm;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JVMLaunchers
{
    private JVMLaunchers() {}

    public static class VmBuilder<T extends Serializable>
    {
        private VmCallable<T> callable;
        private final List<URL> tmpJars = new ArrayList<>();

        public VmBuilder<T> setCallable(VmCallable<T> callable)
        {
            this.callable = requireNonNull(callable, "callable is null");
            return this;
        }

        public VmBuilder<T> addUserURLClassLoader(URLClassLoader classLoader)
        {
            Collections.addAll(tmpJars, classLoader.getURLs());
            return this;
        }

        public VmBuilder<T> addUserjars(List<URL> jars)
        {
            tmpJars.addAll(jars);
            return this;
        }

        public JVMLauncher<T> build()
        {
            return new JVMLauncher<T>(callable, tmpJars);
        }
    }

    public static <T extends Serializable> VmBuilder<T> newJvm()
    {
        return new VmBuilder<T>();
    }
}
