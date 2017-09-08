package cn.edu.tsinghua.iotdb.exception;

/**
 * OverflowOpType could only be INSERT, UPDATE, DELETE.
 * The other situations would throw this exception.
 *
 * @author CGF
 */
public class UnSupportedOverflowOpTypeException extends DeltaEngineRunningException{
	private static final long serialVersionUID = -3834482432038784174L;

	public UnSupportedOverflowOpTypeException(String message, Throwable cause) { super(message, cause);}

    public UnSupportedOverflowOpTypeException(String message) {
        super(message);
    }

    public UnSupportedOverflowOpTypeException(Throwable cause) {
        super(cause);
    }
}
