package ideal.sylph.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Repeatable(Name.Names.class)   //java8 多重注解
@Retention(RetentionPolicy.RUNTIME)
public @interface Name
{
    String value();

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Names
    {
        Name[] value();
    }
}
