package cn.edu.tsinghua.iotdb.exception;


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
