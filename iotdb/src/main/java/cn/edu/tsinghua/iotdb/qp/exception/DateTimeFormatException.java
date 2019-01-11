package cn.edu.tsinghua.iotdb.qp.exception;

import cn.edu.tsinghua.iotdb.exception.qp.QueryProcessorException;

public class DateTimeFormatException extends QueryProcessorException {
	private static final long serialVersionUID = 5901175084493972130L;

	public DateTimeFormatException(String msg) {
		super(msg);
	}

}
