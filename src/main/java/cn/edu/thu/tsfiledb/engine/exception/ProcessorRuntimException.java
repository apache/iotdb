package cn.edu.thu.tsfiledb.engine.exception;

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
