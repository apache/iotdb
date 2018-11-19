package cn.edu.tsinghua.iotdb.exception;

/**
 *
 * This Exception is the parent class for all delta engine runtime exceptions.<br>
 * This Exception extends super class {@link java.lang.RuntimeException}
 *
 * @author CGF
 */
public abstract class DeltaEngineRunningException extends RuntimeException{
	private static final long serialVersionUID = 7537799061005397794L;

	public DeltaEngineRunningException() { super();}

    public DeltaEngineRunningException(String message, Throwable cause) { super(message, cause);}

    public DeltaEngineRunningException(String message) {
        super(message);
    }

    public DeltaEngineRunningException(Throwable cause) {
        super(cause);
    }

}
