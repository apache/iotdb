package org.apache.iotdb.db.exception;


/**
 * @author liukun
 *
 */
public class ProcessorRuntimException extends RuntimeException {

	private static final long serialVersionUID = -5543549255867713835L;

	public ProcessorRuntimException() {
		super();
	}

	public ProcessorRuntimException(String message) {
		super(message);
	}

	public ProcessorRuntimException(Throwable cause) {
		super(cause);
	}

	
}
