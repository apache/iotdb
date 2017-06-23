package cn.edu.thu.tsfiledb.qp.exception;

import cn.edu.thu.tsfiledb.qp.logical.crud.UpdateOperator;

/**
 * This exception is threw while meeting error in
 * {@linkplain UpdateOperator UpdateOperator}
 * 
 * @author kangrong
 *
 */
public class GeneratePhysicalPlanException extends QueryProcessorException {

    private static final long serialVersionUID = -797390809639488007L;

    public GeneratePhysicalPlanException(String msg) {
        super(msg);
    }

}
