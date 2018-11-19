package cn.edu.tsinghua.iotdb.exception;

import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

public class FileNodeProcessorException extends ProcessorException {

	private static final long serialVersionUID = 7373978140952977661L;

	public FileNodeProcessorException() {
		super();
	}

	public FileNodeProcessorException(PathErrorException pathExcp) {
		super(pathExcp.getMessage());
	}

	public FileNodeProcessorException(String msg) {
		super(msg);
	}

	public FileNodeProcessorException(Throwable throwable) {
		super(throwable.getMessage());
	}
}
