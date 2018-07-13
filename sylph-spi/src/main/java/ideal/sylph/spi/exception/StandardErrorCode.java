package ideal.sylph.spi.exception;

import static ideal.sylph.spi.exception.ErrorType.EXTERNAL;
import static ideal.sylph.spi.exception.ErrorType.INTERNAL_ERROR;
import static ideal.sylph.spi.exception.ErrorType.USER_ERROR;

public enum StandardErrorCode
{
    CONNECTION_ERROR(1, EXTERNAL),   //yarn 连接错误
    CONFIG_ERROR(2, INTERNAL_ERROR),    // 配置错误
    SYSTEM_ERROR(3, INTERNAL_ERROR),    //系统错误
    LOAD_MODULE_ERROR(4, INTERNAL_ERROR),    // 模块加载失败 错误

    JOB_START_ERROR(5, INTERNAL_ERROR),      // job启动失败
    JOB_CONFIG_ERROR(6, USER_ERROR),    // 配置错误
    SAVE_JOB_ERROR(7, EXTERNAL),    // 保存失败
    JOB_BUILD_ERROR(8, EXTERNAL),    // job编译或者装配 错误

    ILLEGAL_OPERATION(9, USER_ERROR),        //非法操作
    NOT_SUPPORTED(0x0000_000D, USER_ERROR), //不支持的功能
    UNKNOWN_ERROR(0x0000_AAAA, INTERNAL_ERROR);  //未知错误 或者没有分类的错误

    private final ErrorCode errorCode;

    StandardErrorCode(int code, ErrorType type)
    {
        this.errorCode = new ErrorCode(code, name(), type);
    }

    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    public boolean equals(StandardErrorCode errCode)
    {
        return this.errorCode.equals(errCode.errorCode);
    }

    @Override
    public String toString()
    {
        return new StringBuilder(errorCode.getName())
                .append(":").append(errorCode.getType())
                .append(":").append(errorCode.getCode()).toString();
    }
}
