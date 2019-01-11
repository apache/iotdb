package cn.edu.tsinghua.iotdb.exception.qp;

import cn.edu.tsinghua.iotdb.qp.logical.crud.UpdateOperator;

/**
 * This exception is threw while meeting error in
 * {@linkplain UpdateOperator UpdateOperator}
 */
public class GeneratePhysicalPlanException extends QueryProcessorException {

    private static final long serialVersionUID = -797390809639488007L;

    public GeneratePhysicalPlanException(String msg) {
        super(msg);
    }

}
