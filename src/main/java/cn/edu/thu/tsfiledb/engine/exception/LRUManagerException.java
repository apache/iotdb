package cn.edu.thu.tsfiledb.engine.exception;

public class LRUManagerException extends Exception {

	private static final long serialVersionUID = 3426203851432238314L;

	public LRUManagerException() {
		super();
	}

	public LRUManagerException(String message) {
		super(message);
	}

	public LRUManagerException(Throwable cause) {
		super(cause);
	}

}
