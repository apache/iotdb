package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class QpSelectFromException extends QueryProcessorException {
    private static final long serialVersionUID = -8987543591129315588L;

    public QpSelectFromException(String msg) {
        super(msg);
    }

}
