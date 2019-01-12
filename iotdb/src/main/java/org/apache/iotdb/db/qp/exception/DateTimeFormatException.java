package org.apache.iotdb.db.qp.exception;

import org.apache.iotdb.db.exception.qp.QueryProcessorException;

public class DateTimeFormatException extends QueryProcessorException {
	private static final long serialVersionUID = 5901175084493972130L;

	public DateTimeFormatException(String msg) {
		super(msg);
	}

}
