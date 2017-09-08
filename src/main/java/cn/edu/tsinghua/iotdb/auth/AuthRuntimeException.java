package cn.edu.tsinghua.iotdb.auth;

/**
 * @author liukun
 *
 */
public class AuthRuntimeException extends RuntimeException {

	private static final long serialVersionUID = -4455372256207220995L;

	public AuthRuntimeException() {
		super();
	}

	public AuthRuntimeException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public AuthRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public AuthRuntimeException(String message) {
		super(message);
	}

	public AuthRuntimeException(Throwable cause) {
		super(cause);
	}
	
	
}
