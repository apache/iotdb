package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw.InsertOperator InsertOperator}
 * 
 * @author kangrong
 *
 */
public class InsertOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = -6280121806646850135L;

    public InsertOperatorException(String msg) {
        super(msg);
    }

}
