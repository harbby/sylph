package ideal.sylph.common.jvm;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JVMUtil
{
    private JVMUtil() {}

    /**
     * 当前class.path里面所有的jar
     */
    public static Set<File> systemJars()
    {
        String[] jars = System.getProperty("java.class.path")
                .split(Pattern.quote(File.pathSeparator));
        Set<File> res = Arrays.stream(jars).map(File::new).filter(File::isFile)
                .collect(Collectors.toSet());
        //res.forEach(x -> logger.info("systemJars: {}", x));
        //logger.info("flink job systemJars size: {}", res.size());
        return res;
    }
}
