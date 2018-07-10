package ideal.sylph.spi.exception;

public class SylphException
        extends RuntimeException
{
    private final ErrorCode errorCode;

    public SylphException(StandardErrorCode errorCode, String message)
    {
        this(errorCode, message, null);
    }

    public SylphException(StandardErrorCode errorCode, Throwable throwable)
    {
        this(errorCode, null, throwable);
    }

    public SylphException(StandardErrorCode errorCodeSupplier, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public SylphException(StandardErrorCode errorCodeSupplier, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }
}
