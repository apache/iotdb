package cn.edu.thu.tsfiledb.engine.exception;

import cn.edu.thu.tsfile.common.exception.ProcessorException;

public class LRUManagerException extends ProcessorException {

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
