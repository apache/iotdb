package cn.edu.thu.tsfiledb.exception;

import cn.edu.thu.tsfiledb.exception.ProcessorException;

public class OverflowProcessorException extends ProcessorException {

	private static final long serialVersionUID = -2784502746101925819L;

	public OverflowProcessorException() {
		super();
	}

	public OverflowProcessorException(PathErrorException pathExcp) {
		super(pathExcp);
	}

	public OverflowProcessorException(String msg) {
		super(msg);
	}

	public OverflowProcessorException(Throwable throwable) {
		super(throwable);
	}

	
}
