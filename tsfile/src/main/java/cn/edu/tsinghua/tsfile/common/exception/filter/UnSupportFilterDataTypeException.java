package cn.edu.tsinghua.tsfile.common.exception.filter;

/**
 * Some wrong filter parameters invoke.
 */
public class UnSupportFilterDataTypeException extends RuntimeException {

    public UnSupportFilterDataTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnSupportFilterDataTypeException(String message) {
        super(message);
    }

    public UnSupportFilterDataTypeException(Throwable cause) {
        super(cause);
    }
}
