package cn.edu.thu.tsfiledb.qp.exception.strategy;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class QpWhereException extends TSTransformException {
    private static final long serialVersionUID = -8987915911329315088L;

    public QpWhereException(String msg) {
        super(msg);
    }

}
