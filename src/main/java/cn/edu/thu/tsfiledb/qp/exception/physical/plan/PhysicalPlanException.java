package cn.edu.thu.tsfiledb.qp.exception.physical.plan;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;

public class PhysicalPlanException extends QueryProcessorException {

    private static final long serialVersionUID = 2730849997674197054L;

    public PhysicalPlanException(String msg) {
        super(msg);
    }

}
