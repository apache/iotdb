package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.strategy.QpWhereException;

public class ParseLeafException extends QpWhereException {
    private static final long serialVersionUID = -8987115911329315088L;

    public ParseLeafException(String msg) {
        super(msg);
    }

}
