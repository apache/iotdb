package cn.edu.thu.tsfiledb.engine.exception;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.exception.ProcessorException;

public class FileNodeProcessorException extends ProcessorException {

	private static final long serialVersionUID = 7373978140952977661L;

	public FileNodeProcessorException() {
		super();
	}

	public FileNodeProcessorException(PathErrorException pathExcp) {
		super(pathExcp);
	}

	public FileNodeProcessorException(String msg) {
		super(msg);
	}

	public FileNodeProcessorException(Throwable throwable) {
		super(throwable);
	}
}
