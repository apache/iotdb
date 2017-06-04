package cn.edu.thu.tsfiledb.qp.logical.optimizer;

import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;

/**
 * This class do nothing for an input TSPlanContext.
 * 
 * @author kangrong
 *
 */
public class NoneLogicalOptimizer implements ILogicalOptimizer {
    
    @Override
    public Operator transform(Operator context) {
        return context;
    }

}
