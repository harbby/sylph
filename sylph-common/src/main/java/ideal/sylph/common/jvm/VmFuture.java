package ideal.sylph.common.jvm;

import ideal.sylph.common.base.SylphSerializable;

import java.io.Serializable;
import java.util.Optional;

public class VmFuture<V extends Serializable>
        implements SylphSerializable
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

    public void setErrorMessage(String errorMessage)
    {
        this.errorMessage = errorMessage;
    }

    public void setResult(Serializable result)
    {
        this.result = (V) result;
    }

    static <V extends Serializable> VmFuture<V> make(Serializable result, String errorMessage)
    {
        VmFuture<V> future = new VmFuture<>();
        future.setResult(result);
        future.setErrorMessage(errorMessage);
        return future;
    }
}
