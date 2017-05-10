package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class ValueParseException extends QueryProcessorException {
    private static final long serialVersionUID = -8987281211329315088L;

    public ValueParseException(String msg) {
        super(msg);
    }

}
