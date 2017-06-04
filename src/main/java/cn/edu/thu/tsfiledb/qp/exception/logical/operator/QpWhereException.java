package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class QpWhereException extends QueryProcessorException {
    private static final long serialVersionUID = -8987915911329315088L;

    public QpWhereException(String msg) {
        super(msg);
    }

}
