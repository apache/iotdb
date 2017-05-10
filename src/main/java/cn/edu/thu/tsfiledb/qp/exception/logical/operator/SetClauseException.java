package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class SetClauseException extends QueryProcessorException {
    private static final long serialVersionUID = -8987915921329315088L;

    public SetClauseException(String msg) {
        super(msg);
    }

}
