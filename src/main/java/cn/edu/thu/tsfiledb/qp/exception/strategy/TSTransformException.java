package cn.edu.thu.tsfiledb.qp.exception.strategy;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

/**
 * This exception is threw while meeting error in transforming a logical operator to a physical
 * plan.
 * 
 * @author kangrong
 *
 */
public class TSTransformException extends QueryProcessorException {


    private static final long serialVersionUID = 7573857366601268706L;

    public TSTransformException(String msg) {
        super(msg);
    }

}
