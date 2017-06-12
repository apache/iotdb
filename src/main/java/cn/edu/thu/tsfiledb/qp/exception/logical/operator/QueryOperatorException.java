package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.root.crud.QueryOperator;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain QueryOperator QueryOperator}
 * 
 * @author kangrong
 *
 */
public class QueryOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = 4466703029858082216L;

    public QueryOperatorException(String msg) {
        super(msg);
    }

}
