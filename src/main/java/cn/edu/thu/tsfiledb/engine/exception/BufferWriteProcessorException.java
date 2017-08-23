package cn.edu.thu.tsfiledb.engine.exception;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

public class BufferWriteProcessorException extends ProcessorException {

	private static final long serialVersionUID = 6817880163296469038L;

	public BufferWriteProcessorException() {
		super();
	}

	public BufferWriteProcessorException(PathErrorException pathExcp) {
		super(pathExcp.getMessage());
	}

	public BufferWriteProcessorException(String msg) {
		super(msg);
	}

	public BufferWriteProcessorException(Throwable throwable) {
		super(throwable.getMessage());
	}
	
	

}
