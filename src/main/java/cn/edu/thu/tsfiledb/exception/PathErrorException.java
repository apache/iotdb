package cn.edu.thu.tsfiledb.exception;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class PathErrorException extends QueryProcessorException {

	private static final long serialVersionUID = 2141197032898163234L;

	public PathErrorException(String msg){
		super(msg);
	}
}
