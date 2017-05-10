package cn.edu.thu.tsfiledb.qp.exception.logical.optimize;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class LogicalOptimizeException extends QueryProcessorException {

    private static final long serialVersionUID = -7098092782689670064L;

    public LogicalOptimizeException(String msg) {
        super(msg);
    }

}
