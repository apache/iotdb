package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain cn.edu.thu.tsfiledb.qp.logical.operator.crud.DeleteOperator DeleteOperator}
 * 
 * @author kangrong
 *
 */
public class DeleteOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = 5532624124231495807L;

    public DeleteOperatorException(String msg) {
        super(msg);
    }

}
