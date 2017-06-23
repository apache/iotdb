package cn.edu.thu.tsfiledb.qp.logical.crud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class presents series condition which is general(e.g. numerical comparison) or defined by
 * user. Function is used for bottom operator.<br>
 * FunctionOperator has a {@code path}, and other filter condition.
 * 
 * @author kangrong
 * @author qiaojialin
 *
 */

public class FunctionOperator extends FilterOperator {
    private Logger LOG = LoggerFactory.getLogger(FunctionOperator.class);

    public FunctionOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.FUNC;
    }

    @Override
    public boolean addChildOperator(FilterOperator op) {
        LOG.error("cannot add child to leaf FilterOperator, now it's FunctionOperator");
        return false;
    }
    
}
