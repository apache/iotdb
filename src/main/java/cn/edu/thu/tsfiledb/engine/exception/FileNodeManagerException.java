package cn.edu.thu.tsfiledb.engine.exception;

public class FileNodeManagerException extends LRUManagerException {

	private static final long serialVersionUID = 9001649171768311032L;

	public FileNodeManagerException() {
		super();
	}

	public FileNodeManagerException(String message) {
		super(message);
	}

	public FileNodeManagerException(Throwable cause) {
		super(cause);
	}
	
	
}
