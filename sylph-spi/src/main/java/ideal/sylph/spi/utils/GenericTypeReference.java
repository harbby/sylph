package ideal.sylph.spi.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import ideal.sylph.common.utils.ParameterizedTypeImpl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * demo:
 * Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));
 */
public class GenericTypeReference
        extends TypeReference<Object>
{
    private ParameterizedType type;

    public GenericTypeReference(Class<?> rawType, Type... typeArguments)
    {
        //this.type = new MoreTypes.ParameterizedTypeImpl(null, rawType, typeArguments);
        this.type = ParameterizedTypeImpl.make(rawType, typeArguments, null);
    }

    @Override
    public java.lang.reflect.Type getType()
    {
        return this.type;
    }
}
