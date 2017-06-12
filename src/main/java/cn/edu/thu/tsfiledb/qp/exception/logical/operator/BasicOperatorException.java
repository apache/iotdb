package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.common.filter.BasicFunctionOperator;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain BasicFunctionOperator BasicFunctionOperator}
 * 
 * @author kangrong
 *
 */
public class BasicOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = -2163809754074237707L;

    public BasicOperatorException(String msg) {
        super(msg);
    }

}
