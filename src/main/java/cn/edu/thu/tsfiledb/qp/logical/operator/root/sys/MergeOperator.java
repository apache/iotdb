package cn.edu.thu.tsfiledb.qp.logical.operator.root.sys;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.RootOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 *  This class is used for Merge clause(not used up to now).
 */
public class MergeOperator extends RootOperator{

	public MergeOperator(int tokenIntType) {
		super(tokenIntType);
	}

	@Override
	public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf) throws QueryProcessorException {
		return null;
	}
}
