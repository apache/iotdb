package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain cn.edu.thu.tsfiledb.qp.logical.operator.crud.UpdateOperator UpdateOperator}
 * 
 * @author kangrong
 *
 */
public class UpdateOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = -797390809639488007L;

    public UpdateOperatorException(String msg) {
        super(msg);
    }

}
