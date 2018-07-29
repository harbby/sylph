package ideal.sylph.common.jvm;

import java.io.Serializable;
import java.util.Optional;

public class VmFuture<V extends Serializable>
        implements Serializable
{
    private V result;
    private String errorMessage;

    public Optional<V> get()
    {
        return Optional.ofNullable(result);
    }

    public String getOnFailure()
    {
        return errorMessage;
    }

    public VmFuture(Serializable result)
    {
        this.result = (V) result;
    }

    public VmFuture(String errorMessage)
    {
        this.errorMessage = errorMessage;
    }

    public VmFuture(Serializable result, String errorMessage)
    {
        this.errorMessage = errorMessage;
    }

    static <V extends Serializable> VmFuture<V> make(Serializable result, String errorMessage)
    {
        return new VmFuture<>(result, errorMessage);
    }
}
