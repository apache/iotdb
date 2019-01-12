package org.apache.iotdb.db.exception.qp;

import org.apache.iotdb.db.qp.logical.crud.UpdateOperator;
import org.apache.iotdb.db.qp.logical.crud.UpdateOperator;

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
