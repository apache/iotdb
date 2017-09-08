package cn.edu.tsinghua.iotdb.exception;

import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

public class BufferWriteProcessorException extends ProcessorException {

	private static final long serialVersionUID = 6817880163296469038L;

	public BufferWriteProcessorException() {
		super();
	}

	public BufferWriteProcessorException(Exception pathExcp) {
		super(pathExcp.getMessage());
	}

	public BufferWriteProcessorException(String msg) {
		super(msg);
	}

	public BufferWriteProcessorException(Throwable throwable) {
		super(throwable.getMessage());
	}
	
	

}
